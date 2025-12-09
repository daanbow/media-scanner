"""
Professional Media Library Scanner v1.0 (Production & Databricks-ready)

- Multi-threaded scanner for large media libraries (NAS / local).
- Rich technical metadata via MediaInfo + EXIF.
- TMDB enrichment with safe handling of missing / weird data.
- Multi-language-aware (PL / EN / CS / ES / DE / FR / IT).
- Output: CSV optimized for Databricks ingestion (no bad rows).

Author: daanbow & AI (Data Engineering Team)
"""

import os
import re
import json
import time
import math
import logging
import warnings
import csv
from typing import Any, Dict, List, Optional, Tuple, Set
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from enum import Enum
from threading import Lock

import pandas as pd
import requests
from pymediainfo import MediaInfo

# Optional Pillow for image EXIF
HAS_PIL = False
try:
    from PIL import Image
    from PIL.ExifTags import TAGS

    # Suppress noisy TIFF EXIF warnings
    warnings.filterwarnings("ignore", category=UserWarning, module="PIL.TiffImagePlugin")
    HAS_PIL = True
except ImportError:
    HAS_PIL = False

from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TimeRemainingColumn,
    MofNCompleteColumn,
    SpinnerColumn,
)
from rich.console import Console
from rich.logging import RichHandler
from rich.traceback import install

# Better tracebacks in console
install(show_locals=True)
console = Console()


# =================== CONFIGURATION ===================

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


class Config:
    """Production configuration."""

    # Media root – możesz nadpisywać przez env: MEDIA_ROOT=E:\\MEDIA albo \\NAS\share
    MEDIA_ROOT: str = os.getenv("MEDIA_ROOT", r"E:\MEDIA").strip()

    ROOT_DIRECTORIES: List[str] = [
        os.path.join(MEDIA_ROOT, "Christmas"),
        os.path.join(MEDIA_ROOT, "Concert"),
        os.path.join(MEDIA_ROOT, "Docu"),
        os.path.join(MEDIA_ROOT, "Fun"),
        os.path.join(MEDIA_ROOT, "Kids Movies"),
        os.path.join(MEDIA_ROOT, "Kids Series"),
        os.path.join(MEDIA_ROOT, "Movies"),
        os.path.join(MEDIA_ROOT, "Movies Extras"),
        os.path.join(MEDIA_ROOT, "Movies PL"),
        os.path.join(MEDIA_ROOT, "Music"),
        os.path.join(MEDIA_ROOT, "Music Clips"),
        os.path.join(MEDIA_ROOT, "Others"),
        os.path.join(MEDIA_ROOT, "Photography"),
        os.path.join(MEDIA_ROOT, "Religious"),
        os.path.join(MEDIA_ROOT, "Series"),
        os.path.join(MEDIA_ROOT, "Series Original"),
        os.path.join(MEDIA_ROOT, "Series Polish"),
        os.path.join(MEDIA_ROOT, "Theater"),
    ]

    ALLOWED_EXTENSIONS: Set[str] = {
        # Video
        ".mp4", ".mkv", ".avi", ".wmv", ".ts", ".m4v", ".mov",
        ".m2ts", ".webm", ".ogv", ".flv", ".vob", ".iso", ".bdmv",
        # Audio
        ".flac", ".mp3", ".m4a", ".wav", ".aac", ".ogg", ".wma",
        # Images
        ".jpg", ".jpeg", ".png", ".tif", ".tiff", ".raw",
        ".cr2", ".nef", ".arw", ".dng",
    }

    # Outputs
    CSV_FILE: str = "media_catalog_databricks.csv"
    CACHE_FILE: str = "media_cache.json"
    LOG_FILE: str = "media_scanner.log"
    ERRORS_FILE: str = "scan_errors.json"
    STATS_FILE: str = "scan_statistics.json"

    # TMDB
    TMDB_BASE_URL: str = "https://api.themoviedb.org/3"
    TMDB_API_KEY: str = os.getenv("TMDB_API_KEY", "").strip()
    TMDB_MAX_RETRIES: int = 3
    TMDB_RATE_LIMIT_DELAY: float = 0.3
    TMDB_REQUEST_TIMEOUT: Tuple[int, int] = (5, 20)

    # Logic
    ENABLE_TMDB: bool = True
    FORCE_TMDB_IF_MISSING: bool = True
    PRUNE_CACHE_MISSING: bool = True

    MAX_WORKERS: int = 4
    CACHE_SAVE_INTERVAL: int = 50
    CACHE_VERSION: str = "1.0"

    NETWORK_RETRY_ATTEMPTS: int = 3
    NETWORK_RETRY_DELAY: float = 2.0

    @classmethod
    def validate(cls) -> None:
        if cls.ENABLE_TMDB and not cls.TMDB_API_KEY:
            logging.warning("WARNING: TMDB_API_KEY not set – TMDB enrichment disabled.")
            cls.ENABLE_TMDB = False

        if not HAS_PIL:
            logging.warning("WARNING: Pillow not installed – image EXIF extraction disabled.")

        if not os.path.exists(cls.MEDIA_ROOT):
            logging.error(f"CRITICAL: Cannot access media root: {cls.MEDIA_ROOT}")
            raise ConnectionError(f"Media root not accessible: {cls.MEDIA_ROOT}")

        accessible = sum(1 for d in cls.ROOT_DIRECTORIES if os.path.exists(d))
        logging.info(
            f"Configuration loaded. Found {accessible}/{len(cls.ROOT_DIRECTORIES)} accessible directories."
        )


# =================== LOGGING ===================

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[
        RichHandler(rich_tracebacks=True, markup=True, show_path=False),
        logging.FileHandler(Config.LOG_FILE, encoding="utf-8", mode="a"),
    ],
)
logger = logging.getLogger("media_scanner")


# =================== DATA MODELS ===================

class MediaType(Enum):
    MOVIE = "movie"
    MOVIE_POLISH = "movie_pl"
    MOVIE_EXTRA = "movie_extra"
    SERIES = "series"
    SERIES_POLISH = "series_pl"
    SERIES_ORIGINAL = "series_original"  # NEW: dedicated type for "Series Original"
    MUSIC = "music"
    MUSIC_CLIP = "music_clip"
    DOCUMENTARY = "documentary"
    CONCERT = "concert"
    KIDS_MOVIE = "kids_movie"
    KIDS_SERIES = "kids_series"
    THEATER = "theater"
    CHRISTMAS = "christmas"
    RELIGIOUS = "religious"
    PHOTOGRAPHY = "photography"
    FUN = "fun"
    MIXED = "mixed"
    UNKNOWN = "unknown"


class ContentLanguage(Enum):
    POLISH = "pl"
    ENGLISH = "en"
    CZECH = "cs"
    SPANISH = "es"
    GERMAN = "de"
    FRENCH = "fr"
    ITALIAN = "it"
    ORIGINAL = "original"
    MIXED = "mixed"
    UNKNOWN = "unknown"


@dataclass
class MediaRecord:
    # Identity & Location
    file_path: str
    filename: str
    folder_name: str
    extension: str
    source_root: str
    media_type: str
    content_language: str

    # Filesystem
    size_bytes: int
    size_mb: float
    size_gb: float
    created_at_ts: str
    modified_at_ts: str
    scanned_at_ts: str

    # Parsed from name
    extracted_year: Optional[int] = None
    extracted_title: Optional[str] = None
    season_number: Optional[int] = None
    episode_number: Optional[int] = None
    parsed_resolution: Optional[str] = None
    parsed_source: Optional[str] = None

    # Collection/Series
    collection_name: Optional[str] = None
    track_name: Optional[str] = None
    performer: Optional[str] = None
    content_description: Optional[str] = None
    recorded_date: Optional[str] = None
    part_id: Optional[int] = None

    # Video technical
    duration_sec: Optional[float] = None
    duration_fmt: Optional[str] = None
    video_codec: Optional[str] = None
    video_profile: Optional[str] = None
    video_resolution: Optional[str] = None
    width: Optional[int] = None
    height: Optional[int] = None
    aspect_ratio: Optional[str] = None
    frame_rate: Optional[float] = None
    video_bitrate_kbps: Optional[float] = None
    video_bit_rate_max_kbps: Optional[float] = None
    video_bit_depth: Optional[int] = None
    scan_type: Optional[str] = None
    color_space: Optional[str] = None
    source_duration_sec: Optional[float] = None
    timecode_first_frame: Optional[str] = None

    # HDR
    hdr_format_commercial: Optional[str] = None
    color_primaries: Optional[str] = None
    transfer_characteristics: Optional[str] = None
    matrix_coefficients: Optional[str] = None
    mastering_display_primaries: Optional[str] = None
    mastering_display_luminance: Optional[str] = None
    max_content_light_level: Optional[int] = None
    max_frame_avg_light_level: Optional[int] = None

    # Audio tracks info
    audio_main_codec: Optional[str] = None
    audio_main_channels: Optional[str] = None
    audio_main_language: Optional[str] = None
    audio_main_bitrate_kbps: Optional[float] = None
    audio_count: int = 0
    audio_languages_list: Optional[str] = None
    audio_formats: Optional[str] = None
    audio_commercial_names: Optional[str] = None
    audio_bitrates: Optional[str] = None
    audio_channels_list: Optional[str] = None
    audio_titles: Optional[str] = None
    audio_service_kinds: Optional[str] = None
    audio_delays_ms: Optional[str] = None

    # Subtitles
    subtitle_count: int = 0
    subtitle_languages_list: Optional[str] = None
    subtitle_formats: Optional[str] = None
    subtitle_titles: Optional[str] = None
    subtitle_event_counts: Optional[str] = None
    subtitle_muxing_modes: Optional[str] = None
    subtitle_forced_count: int = 0

    # Container / Encoding
    container_format: Optional[str] = None
    format_version: Optional[str] = None
    overall_bitrate_kbps: Optional[float] = None
    matroska_unique_id: Optional[str] = None
    movie_name: Optional[str] = None
    error_detection_type: Optional[str] = None
    has_attachments: int = 0
    attachment_list: Optional[str] = None
    writing_library: Optional[str] = None
    writing_application: Optional[str] = None
    encoding_settings: Optional[str] = None
    encoded_date: Optional[str] = None

    # AI & Chapters
    ai_enhanced: int = 0
    ai_processing_info: Optional[str] = None
    has_chapters: int = 0
    chapter_count: int = 0

    # Image
    image_width: Optional[int] = None
    image_height: Optional[int] = None
    image_megapixels: Optional[float] = None
    camera_make: Optional[str] = None
    camera_model: Optional[str] = None
    date_taken: Optional[str] = None

    # TMDB
    tmdb_id: Optional[int] = None
    tmdb_title: Optional[str] = None
    tmdb_original_title: Optional[str] = None
    tmdb_rating: Optional[float] = None
    tmdb_vote_count: Optional[int] = None
    tmdb_genres: Optional[str] = None
    tmdb_release_date: Optional[str] = None
    tmdb_overview: Optional[str] = None
    tmdb_runtime: Optional[int] = None
    tmdb_production_countries: Optional[str] = None
    tmdb_poster_path: Optional[str] = None

    # Quality flags (0/1)
    is_4k: int = 0
    has_hdr: int = 0
    has_dolby_vision: int = 0
    has_atmos: int = 0
    has_dts_x: int = 0
    has_lossless_audio: int = 0

    has_polish_audio: int = 0
    has_english_audio: int = 0
    has_czech_audio: int = 0
    has_spanish_audio: int = 0
    has_german_audio: int = 0
    has_french_audio: int = 0
    has_italian_audio: int = 0

    has_polish_subtitles: int = 0
    has_english_subtitles: int = 0
    has_czech_subtitles: int = 0
    has_spanish_subtitles: int = 0
    has_german_subtitles: int = 0
    has_french_subtitles: int = 0
    has_italian_subtitles: int = 0

    is_polish_production: int = 0
    is_multi_audio: int = 0
    has_subtitles: int = 0
    has_ai_upscaling: int = 0
    has_cover_art: int = 0

    quality_score: Optional[float] = None
    processing_error: Optional[str] = None
    tmdb_enriched: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =================== PARSERS ===================

class FilenameParser:
    YEAR_PATTERN = re.compile(r"\b(19[2-9]\d|20[0-2]\d)\b")
    SEASON_EP_PATTERN = re.compile(
        r"(?:[sS](\d{1,2})[eE](\d{1,3}))|(?:(\d{1,2})x(\d{1,3}))", re.IGNORECASE
    )

    RES_PATTERNS = {
        "8K": re.compile(r"8k|4320p", re.IGNORECASE),
        "4K": re.compile(r"4k|2160p|uhd", re.IGNORECASE),
        "1080p": re.compile(r"1080p|fhd", re.IGNORECASE),
        "720p": re.compile(r"720p|hd(?!tv)", re.IGNORECASE),
        "480p": re.compile(r"480p|sd", re.IGNORECASE),
    }

    SOURCE_PATTERNS = {
        "BluRay": re.compile(r"blu-?ray|bdrip|brrip|remux", re.IGNORECASE),
        "Web-DL": re.compile(r"web-?dl|webrip|vod", re.IGNORECASE),
        "HDTV": re.compile(r"hdtv|pdtv", re.IGNORECASE),
        "DVD": re.compile(r"dvd-?rip|dvd", re.IGNORECASE),
    }

    @staticmethod
    def extract_year(text: str) -> Optional[int]:
        if not text:
            return None
        matches = FilenameParser.YEAR_PATTERN.findall(text)
        if matches:
            current_year = datetime.now().year + 2
            valid = [int(y) for y in matches if 1920 <= int(y) <= current_year]
            return valid[-1] if valid else None
        return None

    @staticmethod
    def extract_season_episode(text: str) -> Tuple[Optional[int], Optional[int]]:
        if not text:
            return None, None
        m = FilenameParser.SEASON_EP_PATTERN.search(text)
        if not m:
            return None, None
        s = m.group(1) or m.group(3)
        e = m.group(2) or m.group(4)
        return int(s), int(e)

    @staticmethod
    def extract_clean_title(filename: str) -> str:
        title = os.path.splitext(filename)[0]
        title = FilenameParser.YEAR_PATTERN.sub("", title)
        title = FilenameParser.SEASON_EP_PATTERN.sub("", title)
        title = re.sub(
            r"\b(1080p|720p|4k|2160p|bluray|bdrip|brrip|remux|web-?dl|x264|x265|hevc|h264|h265|"
            r"aac|ac3|dts|pl|lektor|dubbing|dub|subbed)\b",
            "",
            title,
            flags=re.IGNORECASE,
        )
        title = re.sub(r"[._-]+", " ", title)
        title = re.sub(r"\[.*?\]|\(.*?\)", "", title)
        return " ".join(title.split()).strip()

    @staticmethod
    def parse_quality(text: str) -> Dict[str, Optional[str]]:
        if not text:
            return {"resolution": None, "source": None}
        res = None
        src = None
        for name, pattern in FilenameParser.RES_PATTERNS.items():
            if pattern.search(text):
                res = name
                break
        for name, pattern in FilenameParser.SOURCE_PATTERNS.items():
            if pattern.search(text):
                src = name
                break
        return {"resolution": res, "source": src}


class MediaTypeClassifier:
    MAPPING = {
        "christmas": (MediaType.CHRISTMAS, ContentLanguage.MIXED),
        "concert": (MediaType.CONCERT, ContentLanguage.MIXED),
        "docu": (MediaType.DOCUMENTARY, ContentLanguage.MIXED),
        "fun": (MediaType.FUN, ContentLanguage.MIXED),
        "kids movies": (MediaType.KIDS_MOVIE, ContentLanguage.MIXED),
        "kids series": (MediaType.KIDS_SERIES, ContentLanguage.MIXED),
        "movies": (MediaType.MOVIE, ContentLanguage.ENGLISH),
        "movies extras": (MediaType.MOVIE_EXTRA, ContentLanguage.MIXED),
        "movies pl": (MediaType.MOVIE_POLISH, ContentLanguage.POLISH),
        "music": (MediaType.MUSIC, ContentLanguage.MIXED),
        "music clips": (MediaType.MUSIC_CLIP, ContentLanguage.MIXED),
        "photography": (MediaType.PHOTOGRAPHY, ContentLanguage.UNKNOWN),
        "religious": (MediaType.RELIGIOUS, ContentLanguage.MIXED),
        "others": (MediaType.MIXED, ContentLanguage.MIXED),
        "series": (MediaType.SERIES, ContentLanguage.ENGLISH),
        # NEW: separate media_type for Series Original
        "series original": (MediaType.SERIES_ORIGINAL, ContentLanguage.ORIGINAL),
        "series polish": (MediaType.SERIES_POLISH, ContentLanguage.POLISH),
        "theater": (MediaType.THEATER, ContentLanguage.MIXED),
    }

    @classmethod
    def classify(cls, path: str, ext: str) -> Tuple[MediaType, ContentLanguage]:
        # Images
        if ext in {".jpg", ".jpeg", ".png", ".raw", ".nef", ".cr2", ".arw", ".dng", ".tif", ".tiff"}:
            return MediaType.PHOTOGRAPHY, ContentLanguage.UNKNOWN

        # Audio
        if ext in {".mp3", ".flac", ".wav", ".aac", ".ogg", ".wma", ".m4a"}:
            if "music clip" in path.lower():
                return MediaType.MUSIC_CLIP, ContentLanguage.MIXED
            return MediaType.MUSIC, ContentLanguage.MIXED

        parts = [p.lower() for p in Path(path).parts]
        for key, (media_type, lang) in cls.MAPPING.items():
            if key in parts:
                return media_type, lang

        # Fallback – SxxEyy pattern means series
        if FilenameParser.SEASON_EP_PATTERN.search(os.path.basename(path)):
            return MediaType.SERIES, ContentLanguage.UNKNOWN

        return MediaType.UNKNOWN, ContentLanguage.UNKNOWN


# =================== SAFE FILE OPS ===================

class SafeFileOperations:
    @staticmethod
    def safe_mediainfo_parse(path: str) -> Optional[MediaInfo]:
        for attempt in range(Config.NETWORK_RETRY_ATTEMPTS):
            try:
                return MediaInfo.parse(path, parse_speed=0.25)
            except Exception as e:
                if attempt == Config.NETWORK_RETRY_ATTEMPTS - 1:
                    logger.error(f"MediaInfo failed for {os.path.basename(path)}: {e}")
                    return None
                time.sleep(Config.NETWORK_RETRY_DELAY)
        return None


# =================== METADATA EXTRACTOR ===================

class MetadataExtractor:
    @staticmethod
    def _safe_float(val: Any) -> Optional[float]:
        if val is None:
            return None
        try:
            f = float(val)
            return None if math.isnan(f) else round(f, 2)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_int(val: Any) -> Optional[int]:
        if val is None:
            return None
        try:
            return int(val)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _extract_light_level(text: str) -> Optional[int]:
        if not text:
            return None
        m = re.search(r"(\d+)\s*cd/m[²2]", str(text))
        return int(m.group(1)) if m else None

    @staticmethod
    def _parse_delay_ms(delay_str: str) -> Optional[float]:
        if not delay_str:
            return None
        m = re.search(r"([-+]?\d+(?:\.\d+)?)\s*ms", str(delay_str))
        return float(m.group(1)) if m else None

    @staticmethod
    def _normalize_lang_code(lang: str) -> Optional[str]:
        if not lang:
            return None
        l = lang.lower()
        if l.startswith("pl") or "pol" in l:
            return "pl"
        if l.startswith("en") or "eng" in l:
            return "en"
        if l.startswith("cs") or "cze" in l:
            return "cs"
        if l.startswith("es") or "spa" in l:
            return "es"
        if l.startswith("de") or "ger" in l:
            return "de"
        if l.startswith("fr") or "fre" in l or "fra" in l:
            return "fr"
        if l.startswith("it") or "ita" in l:
            return "it"
        return None

    @staticmethod
    def _calculate_quality_score(record: MediaRecord) -> float:
        score = 0.0
        if record.is_4k:
            score += 30
        elif record.height and record.height >= 1080:
            score += 20
        elif record.height and record.height >= 720:
            score += 10

        if record.has_dolby_vision:
            score += 20
        elif record.has_hdr:
            score += 15

        if record.has_atmos or record.has_dts_x:
            score += 25
        elif record.has_lossless_audio:
            score += 20
        elif record.audio_count > 1:
            score += 10

        if record.overall_bitrate_kbps:
            if record.overall_bitrate_kbps > 50000:
                score += 15
            elif record.overall_bitrate_kbps > 20000:
                score += 10
            elif record.overall_bitrate_kbps > 10000:
                score += 5

        if record.has_polish_subtitles:
            score += 5
        if record.has_ai_upscaling:
            score += 5

        return round(min(score, 100), 1)

    @classmethod
    def extract_image_metadata(cls, path: str) -> Dict[str, Any]:
        meta: Dict[str, Any] = {}
        if not HAS_PIL:
            return meta
        try:
            with Image.open(path) as img:
                meta["image_width"] = img.width
                meta["image_height"] = img.height
                meta["image_megapixels"] = round(
                    (img.width * img.height) / 1_000_000, 2
                )
                exif = img.getexif()
                if exif:
                    for tag_id, value in exif.items():
                        tag = TAGS.get(tag_id, tag_id)
                        if tag == "Make":
                            meta["camera_make"] = str(value).strip()
                        elif tag == "Model":
                            meta["camera_model"] = str(value).strip()
                        elif tag == "DateTimeOriginal":
                            meta["date_taken"] = str(value).strip()
        except Exception:
            pass
        return meta

    @classmethod
    def process_file(
        cls, path: str, root_dir: str, file_info: Tuple[int, float, float]
    ) -> MediaRecord:
        size, mtime, ctime = file_info

        filename = os.path.basename(path)
        folder = os.path.basename(os.path.dirname(path))
        ext = os.path.splitext(filename)[1].lower()

        media_type, lang_from_folder = MediaTypeClassifier.classify(path, ext)
        year = FilenameParser.extract_year(filename) or FilenameParser.extract_year(folder)
        season, episode = FilenameParser.extract_season_episode(filename)
        quality = FilenameParser.parse_quality(filename)
        clean_title = FilenameParser.extract_clean_title(filename)

        record = MediaRecord(
            file_path=path,
            filename=filename,
            folder_name=folder,
            extension=ext,
            source_root=os.path.basename(root_dir),
            media_type=media_type.value,
            content_language=lang_from_folder.value,
            size_bytes=size,
            size_mb=round(size / (1024 * 1024), 2),
            size_gb=round(size / (1024 * 1024 * 1024), 3),
            created_at_ts=datetime.fromtimestamp(ctime).isoformat(),
            modified_at_ts=datetime.fromtimestamp(mtime).isoformat(),
            scanned_at_ts=datetime.now().isoformat(),
            extracted_year=year,
            extracted_title=clean_title,
            season_number=season,
            episode_number=episode,
            parsed_resolution=quality["resolution"],
            parsed_source=quality["source"],
            is_polish_production=1 if lang_from_folder == ContentLanguage.POLISH else 0,
        )

        # Images – EXIF only
        if media_type == MediaType.PHOTOGRAPHY:
            img_meta = cls.extract_image_metadata(path)
            record.image_width = img_meta.get("image_width")
            record.image_height = img_meta.get("image_height")
            record.image_megapixels = img_meta.get("image_megapixels")
            record.camera_make = img_meta.get("camera_make")
            record.camera_model = img_meta.get("camera_model")
            record.date_taken = img_meta.get("date_taken")
            return record

        # Video / Audio – MediaInfo
        try:
            mi = SafeFileOperations.safe_mediainfo_parse(path)
            if not mi:
                record.processing_error = "MediaInfo parse failed"
                return record

            audio_langs: List[str] = []
            audio_formats: List[str] = []
            audio_commercials: List[str] = []
            audio_bitrates: List[str] = []
            audio_channels: List[str] = []
            audio_titles: List[str] = []
            audio_service_kinds: List[str] = []
            audio_delays: List[str] = []

            subtitle_langs: List[str] = []
            subtitle_formats: List[str] = []
            subtitle_titles: List[str] = []
            subtitle_events: List[str] = []
            subtitle_muxing: List[str] = []

            for track in mi.tracks:
                ttype = getattr(track, "track_type", None)

                if ttype == "General":
                    record.container_format = getattr(track, "format", None)
                    record.format_version = getattr(track, "format_version", None)
                    record.writing_application = getattr(
                        track, "writing_application", None
                    )
                    record.encoded_date = getattr(track, "encoded_date", None)
                    record.movie_name = getattr(track, "movie_name", None)
                    record.collection_name = getattr(track, "collection", None)
                    record.track_name = getattr(track, "track_name", None)
                    record.performer = getattr(track, "performer", None)
                    record.content_description = getattr(track, "description", None)
                    record.recorded_date = getattr(track, "recorded_date", None)
                    record.part_id = cls._safe_int(getattr(track, "part_id", None))

                    videoai = getattr(track, "videoai", None)
                    if videoai:
                        record.has_ai_upscaling = 1
                        record.ai_enhanced = 1
                        record.ai_processing_info = str(videoai)[:300]

                    record.error_detection_type = getattr(
                        track, "errordetectiontype", None
                    )
                    attach = getattr(track, "attachments", None)
                    if attach:
                        record.has_attachments = 1
                        record.has_cover_art = 1
                        record.attachment_list = str(attach)

                    uid = getattr(track, "unique_id", None)
                    if uid:
                        record.matroska_unique_id = str(uid)

                    dur_ms = cls._safe_float(getattr(track, "duration", None))
                    if dur_ms:
                        record.duration_sec = round(dur_ms / 1000, 2)
                        record.duration_fmt = time.strftime(
                            "%H:%M:%S", time.gmtime(record.duration_sec)
                        )

                    obr = cls._safe_float(getattr(track, "overall_bit_rate", None))
                    if obr:
                        record.overall_bitrate_kbps = round(obr / 1000, 2)

                elif ttype == "Video":
                    record.video_codec = getattr(track, "format", None)
                    record.video_profile = getattr(track, "format_profile", None)
                    record.writing_library = getattr(track, "writing_library", None)
                    enc_settings = getattr(track, "encoding_settings", None)
                    if enc_settings:
                        record.encoding_settings = str(enc_settings)[:500]

                    w = cls._safe_int(getattr(track, "width", None))
                    h = cls._safe_int(getattr(track, "height", None))
                    if w and h:
                        record.width = w
                        record.height = h
                        record.video_resolution = f"{w}x{h}"
                        if w >= 3800 or h >= 2100:
                            record.is_4k = 1

                    dar = getattr(track, "display_aspect_ratio", None)
                    if dar:
                        record.aspect_ratio = str(dar)

                    record.frame_rate = cls._safe_float(getattr(track, "frame_rate", None))
                    vbr = cls._safe_float(getattr(track, "bit_rate", None))
                    if vbr:
                        record.video_bitrate_kbps = round(vbr / 1000, 2)
                    vbr_max = cls._safe_float(getattr(track, "bit_rate_maximum", None))
                    if vbr_max:
                        record.video_bit_rate_max_kbps = round(vbr_max / 1000, 2)

                    src_dur = cls._safe_float(getattr(track, "source_duration", None))
                    if src_dur:
                        record.source_duration_sec = round(src_dur / 1000, 2)

                    record.timecode_first_frame = getattr(
                        track, "time_code_of_first_frame", None
                    )
                    record.video_bit_depth = cls._safe_int(getattr(track, "bit_depth", None))
                    record.scan_type = getattr(track, "scan_type", None)
                    record.color_space = getattr(track, "color_space", None)

                    record.hdr_format_commercial = getattr(track, "hdr_format", None)
                    record.color_primaries = getattr(track, "color_primaries", None)
                    record.transfer_characteristics = getattr(
                        track, "transfer_characteristics", None
                    )
                    record.matrix_coefficients = getattr(
                        track, "matrix_coefficients", None
                    )
                    record.mastering_display_primaries = getattr(
                        track, "mastering_display_color_primaries", None
                    )

                    lum = getattr(track, "mastering_display_luminance", None)
                    if lum:
                        record.mastering_display_luminance = str(lum)

                    max_cll = getattr(track, "maximum_content_light_level", None)
                    if max_cll:
                        record.max_content_light_level = cls._extract_light_level(
                            str(max_cll)
                        )

                    max_fall = getattr(
                        track, "maximum_frameaverage_light_level", None
                    )
                    if max_fall:
                        record.max_frame_avg_light_level = cls._extract_light_level(
                            str(max_fall)
                        )

                    hdr_fmt = str(record.hdr_format_commercial or "").lower()
                    if "dolby" in hdr_fmt or "vision" in hdr_fmt:
                        record.has_dolby_vision = 1
                        record.has_hdr = 1
                    elif hdr_fmt or (record.video_bit_depth and record.video_bit_depth >= 10):
                        record.has_hdr = 1

                elif ttype == "Audio":
                    record.audio_count += 1
                    lang = getattr(track, "language", "und")
                    fmt = getattr(track, "format", None)
                    commercial = getattr(track, "commercial_name", None)
                    channels = getattr(track, "channel_s", None) or getattr(
                        track, "channels", None
                    )
                    title = getattr(track, "title", None)
                    service_kind = getattr(track, "service_kind", None)
                    delay = getattr(track, "delay_relative_to_video", None)
                    delay_ms = cls._parse_delay_ms(str(delay)) if delay else None

                    if lang and lang not in audio_langs:
                        audio_langs.append(lang)
                    if fmt:
                        audio_formats.append(fmt)
                    if commercial:
                        audio_commercials.append(commercial)
                    if channels:
                        audio_channels.append(f"{channels}ch")
                    if title:
                        audio_titles.append(title)
                    if service_kind:
                        audio_service_kinds.append(service_kind)
                    if delay_ms is not None:
                        audio_delays.append(str(int(delay_ms)))

                    abr = cls._safe_float(getattr(track, "bit_rate", None))
                    if abr:
                        audio_bitrates.append(str(int(abr / 1000)))

                    norm = cls._normalize_lang_code(lang)
                    if norm == "pl":
                        record.has_polish_audio = 1
                    if norm == "en":
                        record.has_english_audio = 1
                    if norm == "cs":
                        record.has_czech_audio = 1
                    if norm == "es":
                        record.has_spanish_audio = 1
                    if norm == "de":
                        record.has_german_audio = 1
                    if norm == "fr":
                        record.has_french_audio = 1
                    if norm == "it":
                        record.has_italian_audio = 1

                    if record.audio_count == 1:
                        record.audio_main_codec = fmt
                        record.audio_main_language = lang
                        record.audio_main_channels = str(channels) if channels else None
                        if abr:
                            record.audio_main_bitrate_kbps = round(abr / 1000, 2)

                    if commercial:
                        cl = commercial.lower()
                        if "atmos" in cl:
                            record.has_atmos = 1
                        if "dts:x" in cl or "dts-x" in cl:
                            record.has_dts_x = 1

                    comp = getattr(track, "compression_mode", "")
                    if "lossless" in str(comp).lower():
                        record.has_lossless_audio = 1
                    if fmt and fmt.upper() in {"FLAC", "ALAC", "MLP FBA", "DTS XLL", "TRUEHD"}:
                        record.has_lossless_audio = 1

                elif ttype == "Text":
                    record.subtitle_count += 1
                    lang = getattr(track, "language", "und")
                    fmt = getattr(track, "format", None)
                    title = getattr(track, "title", None)
                    forced = getattr(track, "forced", None)
                    muxing = getattr(track, "muxing_mode", None)
                    events = getattr(track, "count_of_elements", None)

                    if lang and lang not in subtitle_langs:
                        subtitle_langs.append(lang)
                    if fmt:
                        subtitle_formats.append(fmt)
                    if title:
                        subtitle_titles.append(title)
                    if muxing:
                        subtitle_muxing.append(muxing)
                    if events:
                        subtitle_events.append(str(events))

                    norm = cls._normalize_lang_code(lang)
                    if norm == "pl":
                        record.has_polish_subtitles = 1
                    if norm == "en":
                        record.has_english_subtitles = 1
                    if norm == "cs":
                        record.has_czech_subtitles = 1
                    if norm == "es":
                        record.has_spanish_subtitles = 1
                    if norm == "de":
                        record.has_german_subtitles = 1
                    if norm == "fr":
                        record.has_french_subtitles = 1
                    if norm == "it":
                        record.has_italian_subtitles = 1

                    if forced and str(forced).lower() == "yes":
                        record.subtitle_forced_count += 1

                elif ttype == "Menu":
                    record.has_chapters = 1
                    extra = getattr(track, "extra", {})
                    if isinstance(extra, dict):
                        record.chapter_count = len(
                            [k for k in extra.keys() if str(k).startswith("00:")]
                        )

            # Ustal główny język contentu na podstawie audio
            if audio_langs:
                codes: Set[str] = set()
                for l in audio_langs:
                    norm = cls._normalize_lang_code(l)
                    if norm:
                        codes.add(norm)

                if len(codes) == 1:
                    code = next(iter(codes))
                    if code == "pl":
                        record.content_language = ContentLanguage.POLISH.value
                    elif code == "en":
                        record.content_language = ContentLanguage.ENGLISH.value
                    elif code == "cs":
                        record.content_language = ContentLanguage.CZECH.value
                    elif code == "es":
                        record.content_language = ContentLanguage.SPANISH.value
                    elif code == "de":
                        record.content_language = ContentLanguage.GERMAN.value
                    elif code == "fr":
                        record.content_language = ContentLanguage.FRENCH.value
                    elif code == "it":
                        record.content_language = ContentLanguage.ITALIAN.value
                elif len(codes) > 1:
                    record.content_language = ContentLanguage.MIXED.value

            # Post-process lists
            record.audio_languages_list = ",".join(audio_langs) if audio_langs else None
            record.audio_formats = ",".join(audio_formats) if audio_formats else None
            record.audio_commercial_names = (
                ",".join(audio_commercials) if audio_commercials else None
            )
            record.audio_bitrates = ",".join(audio_bitrates) if audio_bitrates else None
            record.audio_channels_list = (
                ",".join(audio_channels) if audio_channels else None
            )
            record.audio_titles = ",".join(audio_titles) if audio_titles else None
            record.audio_service_kinds = (
                ",".join(audio_service_kinds) if audio_service_kinds else None
            )
            record.audio_delays_ms = ",".join(audio_delays) if audio_delays else None

            record.subtitle_languages_list = (
                ",".join(subtitle_langs) if subtitle_langs else None
            )
            record.subtitle_formats = (
                ",".join(subtitle_formats) if subtitle_formats else None
            )
            record.subtitle_titles = (
                ",".join(subtitle_titles) if subtitle_titles else None
            )
            record.subtitle_event_counts = (
                ",".join(subtitle_events) if subtitle_events else None
            )
            record.subtitle_muxing_modes = (
                ",".join(subtitle_muxing) if subtitle_muxing else None
            )

            record.is_multi_audio = 1 if record.audio_count > 1 else 0
            record.has_subtitles = 1 if record.subtitle_count > 0 else 0
            record.quality_score = cls._calculate_quality_score(record)

        except Exception as e:
            logger.error(f"Extraction error for {filename}: {e}")
            record.processing_error = str(e)

        return record


# =================== TMDB WRAPPER ===================

class TMDBManager:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "MediaScanner/3.6"})
        self.last_request_time = 0.0
        self.request_count = 0
        self.error_count = 0

    def _rate_limit(self) -> None:
        elapsed = time.time() - self.last_request_time
        if elapsed < Config.TMDB_RATE_LIMIT_DELAY:
            time.sleep(Config.TMDB_RATE_LIMIT_DELAY - elapsed)
        self.last_request_time = time.time()

    def _request(self, url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        for attempt in range(Config.TMDB_MAX_RETRIES):
            try:
                self._rate_limit()
                r = self.session.get(url, params=params, timeout=Config.TMDB_REQUEST_TIMEOUT)
                self.request_count += 1
                if r.status_code == 429:
                    time.sleep(2 ** attempt)
                    continue
                r.raise_for_status()
                return r.json()
            except requests.RequestException as e:
                self.error_count += 1
                if attempt == Config.TMDB_MAX_RETRIES - 1:
                    logger.warning(f"TMDB request failed: {e}")
                    return None
                time.sleep(1)
        return None

    def enrich(self, record: MediaRecord) -> MediaRecord:
        if not Config.ENABLE_TMDB:
            return record
        if record.media_type in {
            MediaType.PHOTOGRAPHY.value,
            MediaType.MUSIC.value,
            MediaType.MUSIC_CLIP.value,
        }:
            return record
        if not record.extracted_title:
            return record

        is_series = (
            record.media_type
            in {
                MediaType.SERIES.value,
                MediaType.SERIES_POLISH.value,
                MediaType.SERIES_ORIGINAL.value,  # NEW: treat Series Original as series for TMDB
                MediaType.KIDS_SERIES.value,
            }
            or record.season_number is not None
        )
        endpoint = "tv" if is_series else "movie"

        lang = "pl-PL" if record.is_polish_production else "en-US"
        params: Dict[str, Any] = {
            "api_key": Config.TMDB_API_KEY,
            "query": record.extracted_title,
            "language": lang,
        }
        if record.extracted_year and not is_series:
            params["year"] = record.extracted_year

        data = self._request(f"{Config.TMDB_BASE_URL}/search/{endpoint}", params)
        if (not data or not data.get("results")) and "year" in params:
            params.pop("year", None)
            data = self._request(f"{Config.TMDB_BASE_URL}/search/{endpoint}", params)

        if not data or not data.get("results"):
            return record

        best = data["results"][0]
        record.tmdb_id = best.get("id")

        details_lang = "pl-PL" if record.is_polish_production else "en-US"
        details = self._request(
            f"{Config.TMDB_BASE_URL}/{endpoint}/{record.tmdb_id}",
            {"api_key": Config.TMDB_API_KEY, "language": details_lang},
        )
        if not details:
            return record

        record.tmdb_title = details.get("title") or details.get("name")
        record.tmdb_original_title = (
            details.get("original_title") or details.get("original_name")
        )
        record.tmdb_rating = details.get("vote_average")
        record.tmdb_vote_count = details.get("vote_count")
        record.tmdb_genres = ",".join(
            [g.get("name", "") for g in details.get("genres", [])]
        ) or None
        record.tmdb_release_date = details.get("release_date") or details.get(
            "first_air_date"
        )
        record.tmdb_overview = details.get("overview")

        # SAFE runtime logic – zero IndexError
        runtime = details.get("runtime")
        if runtime is None:
            er = details.get("episode_run_time")
            if isinstance(er, list) and er:
                runtime = er[0]
        record.tmdb_runtime = runtime

        record.tmdb_production_countries = ",".join(
            [c.get("iso_3166_1", "") for c in details.get("production_countries", [])]
        ) or None
        record.tmdb_poster_path = details.get("poster_path")
        record.tmdb_enriched = 1

        return record


# =================== CACHE & ERRORS ===================

class CacheManager:
    def __init__(self) -> None:
        self.path = Config.CACHE_FILE
        self.lock = Lock()
        self.cache = self._load()
        self.dirty = False
        self.hits = 0
        self.misses = 0

    def _load(self) -> Dict[str, Any]:
        if not os.path.exists(self.path):
            return {"version": Config.CACHE_VERSION, "entries": {}}
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                c = json.load(f)
            if c.get("version") != Config.CACHE_VERSION:
                logger.info(
                    f"Cache version mismatch (found {c.get('version')}, expected {Config.CACHE_VERSION}) – resetting cache."
                )
                return {"version": Config.CACHE_VERSION, "entries": {}}
            return c
        except Exception as e:
            logger.warning(f"Failed to load cache – starting fresh: {e}")
            return {"version": Config.CACHE_VERSION, "entries": {}}

    def save(self) -> None:
        with self.lock:
            if not self.dirty:
                return
            try:
                with open(self.path, "w", encoding="utf-8") as f:
                    json.dump(self.cache, f, ensure_ascii=False, indent=2)
                self.dirty = False
            except Exception as e:
                logger.error(f"Cache save failed: {e}")

    def get(self, path: str, mtime: float, size: int) -> Optional[Dict[str, Any]]:
        with self.lock:
            entry = self.cache["entries"].get(path)
            if entry and entry.get("mtime") == mtime and entry.get("size") == size:
                self.hits += 1
                return entry.get("data")
            self.misses += 1
            return None

    def set(self, path: str, mtime: float, size: int, data: Dict[str, Any]) -> None:
        with self.lock:
            self.cache["entries"][path] = {"mtime": mtime, "size": size, "data": data}
            self.dirty = True

    def prune(self, valid_paths: Set[str]) -> None:
        if not Config.PRUNE_CACHE_MISSING:
            return
        with self.lock:
            current = set(self.cache["entries"].keys())
            to_delete = current - valid_paths
            if not to_delete:
                return
            for p in to_delete:
                del self.cache["entries"][p]
            self.dirty = True
            self.save()
            logger.info(f"Pruned {len(to_delete)} cache entries.")

    def stats(self) -> Dict[str, Any]:
        with self.lock:
            total = self.hits + self.misses
            rate = round(self.hits / total * 100, 1) if total else 0.0
            return {"hits": self.hits, "misses": self.misses, "rate": rate}


class ErrorTracker:
    def __init__(self) -> None:
        self.errors: List[Dict[str, Any]] = []
        self.lock = Lock()

    def add(self, path: str, error: Exception, stage: str) -> None:
        with self.lock:
            self.errors.append(
                {
                    "file": os.path.basename(path),
                    "path": path,
                    "error": str(error),
                    "stage": stage,
                    "time": datetime.now().isoformat(),
                }
            )
            logger.error(f"{stage} error for {os.path.basename(path)}: {error}")

    def save(self) -> None:
        with self.lock:
            if not self.errors:
                return
            try:
                with open(Config.ERRORS_FILE, "w", encoding="utf-8") as f:
                    json.dump(self.errors, f, indent=2, ensure_ascii=False)
            except Exception as e:
                logger.error(f"Failed to write error log: {e}")


# =================== CSV SANITIZATION (for Databricks) ===================

def sanitize_dataframe_for_csv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare DataFrame for safe CSV export into Spark/Databricks.

    - removes any newlines from *all* string columns (Spark multiLine=false),
    - leaves None/NaN values as-is,
    - keeps commas, but we will quote all fields on write.
    """
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(
                lambda v: re.sub(r"[\r\n]+", " ", v) if isinstance(v, str) else v
            )
    return df


# =================== MAIN WORKFLOW ===================

def collect_files() -> List[Tuple[str, str, int, float, float]]:
    files: List[Tuple[str, str, int, float, float]] = []
    with console.status("[bold blue]Indexing media structure...") as status:
        for root_dir in Config.ROOT_DIRECTORIES:
            if not os.path.exists(root_dir):
                logger.warning(f"Root directory not accessible: {root_dir}")
                continue
            status.update(f"[bold blue]Scanning: {os.path.basename(root_dir)}[/bold blue]")
            try:
                for root, _, filenames in os.walk(root_dir):
                    for name in filenames:
                        ext = os.path.splitext(name)[1].lower()
                        if ext not in Config.ALLOWED_EXTENSIONS:
                            continue
                        full_path = os.path.join(root, name)
                        try:
                            s = os.stat(full_path)
                        except OSError:
                            continue
                        files.append((full_path, root_dir, s.st_size, s.st_mtime, s.st_ctime))
            except Exception as e:
                logger.error(f"Error walking {root_dir}: {e}")
    logger.info(f"Indexed {len(files)} files.")
    return files


def process_item(
    file_data: Tuple[str, str, int, float, float],
    cache: CacheManager,
    tmdb: TMDBManager,
    errors: ErrorTracker,
) -> Optional[Dict[str, Any]]:
    path, root, size, mtime, ctime = file_data
    try:
        cached = cache.get(path, mtime, size)
        needs_tmdb = (
            Config.ENABLE_TMDB
            and Config.FORCE_TMDB_IF_MISSING
            and cached is not None
            and not cached.get("tmdb_enriched")
        )

        if cached is not None and not needs_tmdb:
            return cached

        record = MetadataExtractor.process_file(path, root, (size, mtime, ctime))

        if Config.ENABLE_TMDB:
            try:
                record = tmdb.enrich(record)
            except Exception as e:
                errors.add(path, e, "tmdb")

        data = record.to_dict()
        cache.set(path, mtime, size, data)
        return data
    except Exception as e:
        errors.add(path, e, "processing")
        return None


def main() -> None:
    console.print("[bold green]Media Scanner v3.6 (multi-language, TMDB-safe)[/bold green]")
    try:
        Config.validate()
    except Exception as e:
        console.print(f"[red]Config error: {e}[/red]")
        return

    cache = CacheManager()
    tmdb = TMDBManager()
    errors = ErrorTracker()

    files = collect_files()
    if not files:
        console.print("[yellow]No media files found. Check MEDIA_ROOT and folders.[/yellow]")
        return

    results: List[Dict[str, Any]] = []
    valid_paths: Set[str] = set()

    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("Processing files...", total=len(files))

        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            futures = {
                executor.submit(process_item, f, cache, tmdb, errors): f[0]
                for f in files
            }
            for future in as_completed(futures):
                path = futures[future]
                valid_paths.add(path)
                try:
                    res = future.result()
                    if res is not None:
                        results.append(res)
                        if len(results) % Config.CACHE_SAVE_INTERVAL == 0:
                            cache.save()
                except Exception as e:
                    errors.add(path, e, "future")
                finally:
                    progress.update(task, advance=1)

    cache.save()
    cache.prune(valid_paths)
    errors.save()

    if not results:
        console.print("[red]No records produced – check logs for errors.[/red]")
        return

    df = pd.DataFrame(results)
    df = sanitize_dataframe_for_csv(df)

    # CSV zapisany w trybie „max safety” – wszystkie pola w cudzysłowach,
    # bez znaków nowej linii w żadnej kolumnie.
    df.to_csv(
        Config.CSV_FILE,
        index=False,
        encoding="utf-8-sig",
        quoting=csv.QUOTE_ALL,
        quotechar='"',
        doublequote=True,
        escapechar="\\",
    )

    stats = cache.stats()
    total_gb = float(df["size_gb"].sum()) if "size_gb" in df.columns else 0.0

    console.print(f"\n[bold green]Done! {len(df)} records saved to {Config.CSV_FILE}[/bold green]")
    console.print(f"Total Size: {total_gb:.2f} GB")
    console.print(
        f"Cache Hit Rate: {stats['rate']}% (hits={stats['hits']}, misses={stats['misses']})"
    )

    try:
        with open(Config.STATS_FILE, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "date": datetime.now().isoformat(),
                    "total": len(df),
                    "size_gb": total_gb,
                    "cache": stats,
                    "errors": len(errors.errors),
                },
                f,
                indent=2,
                ensure_ascii=False,
            )
    except Exception as e:
        logger.error(f"Failed to write stats file: {e}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\n[red]Interrupted by user.[/red]")
