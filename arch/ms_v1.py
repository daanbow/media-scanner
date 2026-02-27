"""
Professional Media Library Scanner v1.1 (Production Ready)
Target: Databricks Delta Lake Ingestion via Network Share (SMB)

Architecture:
[Storage (NAS / Local E:\MEDIA)] -> [Python Scanner (Multi-threaded)] -> [CSV Export]
                                  -> [Databricks Autoloader/Delta]

Key Features:
- I/O Optimized: Single 'stat' call per file passed through the pipeline.
- Thread-Safe: Locked cache, TMDB client and error tracking for concurrent execution.
- Rich Metadata: 130+ fields extracted via MediaInfo, Regex and TMDB.
- Spark Ready: Schema-enforced types (integers for booleans, clean timestamps).

Author: Daniel & AI (Data Engineering Team)
Date: December 2025
"""

import os
import re
import json
import time
import math
import logging
from typing import Any, Dict, List, Optional, Tuple, Set
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict, field
from enum import Enum
from collections import defaultdict
from threading import Lock

import pandas as pd
import requests
from pymediainfo import MediaInfo

# Optional Pillow for image EXIF
try:
    from PIL import Image
    from PIL.ExifTags import TAGS
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
    TaskProgressColumn
)
from rich.console import Console
from rich.logging import RichHandler
from rich.traceback import install

# Better tracebacks
install(show_locals=True)

console = Console()

# =================== CONFIGURATION ===================

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


class Config:
    """Production configuration"""

    # Główny katalog mediów:
    # - domyślnie: E:\MEDIA (tak jak w Twoim `PS E:\MEDIA> dir`)
    # - można nadpisać zmienną środowiskową: MEDIA_ROOT=\\192.168.50.135\Nowy1\MEDIA
    MEDIA_ROOT = os.getenv("MEDIA_ROOT", r"E:\MEDIA")

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
        ".cr2", ".nef", ".arw", ".dng"
    }

    # Output files
    CSV_FILE: str = "media_catalog_databricks.csv"
    CACHE_FILE: str = "media_cache_v3.3.json"
    LOG_FILE: str = "media_scanner.log"
    ERRORS_FILE: str = "scan_errors.json"
    STATS_FILE: str = "scan_statistics.json"

    # TMDB API
    TMDB_BASE_URL: str = "https://api.themoviedb.org/3"
    TMDB_API_KEY: str = os.getenv("TMDB_API_KEY", "").strip()
    TMDB_MAX_RETRIES: int = 3
    TMDB_RATE_LIMIT_DELAY: float = 0.3
    TMDB_REQUEST_TIMEOUT: Tuple[int, int] = (5, 20)

    # Processing Logic
    ENABLE_TMDB: bool = True
    FORCE_TMDB_IF_MISSING: bool = True
    PRUNE_CACHE_MISSING: bool = True
    # Worker count (safe for NAS / HDD)
    MAX_WORKERS: int = 4
    CACHE_SAVE_INTERVAL: int = 50
    CACHE_VERSION: str = "3.3"

    # Network Resilience
    NETWORK_RETRY_ATTEMPTS: int = 3
    NETWORK_RETRY_DELAY: float = 2.0

    @classmethod
    def validate(cls):
        """Validate environment and paths"""
        if cls.ENABLE_TMDB and not cls.TMDB_API_KEY:
            logging.warning("WARNING: TMDB_API_KEY not set. API enrichment disabled.")
            cls.ENABLE_TMDB = False

        if not HAS_PIL:
            logging.warning("WARNING: Pillow not installed. Image EXIF extraction disabled.")

        if not os.path.exists(cls.MEDIA_ROOT):
            logging.error(f"CRITICAL: Cannot access media root: {cls.MEDIA_ROOT}")
            raise ConnectionError(f"Media root not accessible: {cls.MEDIA_ROOT}")

        accessible = sum(1 for d in cls.ROOT_DIRECTORIES if os.path.exists(d))
        logging.info(
            f"Configuration loaded. Found {accessible}/{len(cls.ROOT_DIRECTORIES)} accessible directories."
        )


# =================== LOGGING SETUP ===================

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[
        RichHandler(rich_tracebacks=True, markup=True, show_path=False),
        logging.FileHandler(Config.LOG_FILE, encoding='utf-8', mode='a')
    ]
)
logger = logging.getLogger("media_scanner")


# =================== DATA MODELS ===================

class MediaType(Enum):
    """Media Type Schema"""
    MOVIE = "movie"
    MOVIE_POLISH = "movie_pl"
    MOVIE_EXTRA = "movie_extra"
    SERIES = "series"
    SERIES_POLISH = "series_pl"
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
    ORIGINAL = "original"
    MIXED = "mixed"
    UNKNOWN = "unknown"


@dataclass
class MediaRecord:
    """Databricks-optimized Data Class"""

    # Identity & Location
    file_path: str
    filename: str
    folder_name: str
    extension: str
    source_root: str
    media_type: str
    content_language: str

    # Filesystem metadata
    size_bytes: int
    size_mb: float
    size_gb: float
    created_at_ts: str
    modified_at_ts: str
    scanned_at_ts: str

    # Parsed metadata
    extracted_year: Optional[int] = None
    extracted_title: Optional[str] = None
    season_number: Optional[int] = None
    episode_number: Optional[int] = None
    parsed_resolution: Optional[str] = None
    parsed_source: Optional[str] = None

    # Series/Collection metadata
    collection_name: Optional[str] = None
    track_name: Optional[str] = None
    performer: Optional[str] = None
    content_description: Optional[str] = None
    recorded_date: Optional[str] = None
    part_id: Optional[int] = None

    # Video Technical
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

    # HDR Advanced
    hdr_format_commercial: Optional[str] = None
    color_primaries: Optional[str] = None
    transfer_characteristics: Optional[str] = None
    matrix_coefficients: Optional[str] = None
    mastering_display_primaries: Optional[str] = None
    mastering_display_luminance: Optional[str] = None
    max_content_light_level: Optional[int] = None
    max_frame_avg_light_level: Optional[int] = None

    # Audio Technical
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

    # AI Processing
    ai_enhanced: int = 0
    ai_processing_info: Optional[str] = None

    # Chapters
    has_chapters: int = 0
    chapter_count: int = 0

    # Image Metadata
    image_width: Optional[int] = None
    image_height: Optional[int] = None
    image_megapixels: Optional[float] = None
    camera_make: Optional[str] = None
    camera_model: Optional[str] = None
    date_taken: Optional[str] = None

    # TMDB Data
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

    # Quality Flags (1/0 Integer for Spark)
    is_4k: int = 0
    has_hdr: int = 0
    has_dolby_vision: int = 0
    has_atmos: int = 0
    has_dts_x: int = 0
    has_lossless_audio: int = 0
    has_polish_audio: int = 0
    has_polish_subtitles: int = 0
    is_polish_production: int = 0
    is_multi_audio: int = 0
    has_subtitles: int = 0
    has_ai_upscaling: int = 0
    has_cover_art: int = 0

    # Scores & Status
    quality_score: Optional[float] = None
    processing_error: Optional[str] = None
    tmdb_enriched: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =================== PARSERS ===================

class FilenameParser:
    """Regex based extraction"""

    # szerzej niż wcześniej: 1900–2099, a i tak filtrowane do aktualnego roku
    YEAR_PATTERN = re.compile(r'\b(19\d{2}|20\d{2})\b')
    SEASON_EP_PATTERN = re.compile(
        r'(?:[sS](\d{1,2})[eE](\d{1,3}))|(?:(\d{1,2})x(\d{1,3}))',
        re.IGNORECASE,
    )

    RES_PATTERNS = {
        '8K': re.compile(r'8k|4320p', re.IGNORECASE),
        '4K': re.compile(r'4k|2160p|uhd', re.IGNORECASE),
        '1080p': re.compile(r'1080p|fhd', re.IGNORECASE),
        '720p': re.compile(r'720p|hd(?!tv)', re.IGNORECASE),
        '480p': re.compile(r'480p|sd', re.IGNORECASE),
    }

    SOURCE_PATTERNS = {
        'BluRay': re.compile(r'blu-?ray|bdrip|brrip|remux', re.IGNORECASE),
        'Web-DL': re.compile(r'web-?dl|webrip|vod', re.IGNORECASE),
        'HDTV': re.compile(r'hdtv|pdtv', re.IGNORECASE),
        'DVD': re.compile(r'dvd-?rip|dvd', re.IGNORECASE),
    }

    @staticmethod
    def extract_year(text: str) -> Optional[int]:
        matches = FilenameParser.YEAR_PATTERN.findall(text)
        if matches:
            current_year = datetime.now().year + 2
            valid = [int(y) for y in matches if 1900 <= int(y) <= current_year]
            return valid[-1] if valid else None
        return None

    @staticmethod
    def extract_season_episode(text: str) -> Tuple[Optional[int], Optional[int]]:
        match = FilenameParser.SEASON_EP_PATTERN.search(text)
        if match:
            season = match.group(1) or match.group(3)
            episode = match.group(2) or match.group(4)
            return int(season), int(episode)
        return None, None

    @staticmethod
    def extract_clean_title(filename: str) -> str:
        title = os.path.splitext(filename)[0]
        title = FilenameParser.YEAR_PATTERN.sub('', title)
        title = FilenameParser.SEASON_EP_PATTERN.sub('', title)
        title = re.sub(r'[._\-]', ' ', title)
        title = re.sub(
            r'\b(1080p|720p|4k|2160p|bluray|web-?dl|x264|x265|hevc|h264|h265|aac|ac3|dts|pl|lektor|dubbing|subbed)\b',
            '',
            title,
            flags=re.IGNORECASE,
        )
        title = re.sub(r'\[.*?\]|\(.*?\)', '', title)
        return ' '.join(title.split()).strip()

    @staticmethod
    def parse_quality(text: str) -> Dict[str, Optional[str]]:
        resolution = None
        source = None
        for name, pattern in FilenameParser.RES_PATTERNS.items():
            if pattern.search(text):
                resolution = name
                break
        for name, pattern in FilenameParser.SOURCE_PATTERNS.items():
            if pattern.search(text):
                source = name
                break
        return {"resolution": resolution, "source": source}


class MediaTypeClassifier:
    """Classifies media based on folder structure"""

    MAPPING = {
        'christmas': (MediaType.CHRISTMAS, ContentLanguage.MIXED),
        'concert': (MediaType.CONCERT, ContentLanguage.MIXED),
        'docu': (MediaType.DOCUMENTARY, ContentLanguage.MIXED),
        'fun': (MediaType.FUN, ContentLanguage.MIXED),
        'kids movies': (MediaType.KIDS_MOVIE, ContentLanguage.MIXED),
        'kids series': (MediaType.KIDS_SERIES, ContentLanguage.MIXED),
        'movies': (MediaType.MOVIE, ContentLanguage.ENGLISH),
        'movies extras': (MediaType.MOVIE_EXTRA, ContentLanguage.MIXED),
        'movies pl': (MediaType.MOVIE_POLISH, ContentLanguage.POLISH),
        'music': (MediaType.MUSIC, ContentLanguage.MIXED),
        'music clips': (MediaType.MUSIC_CLIP, ContentLanguage.MIXED),
        'photography': (MediaType.PHOTOGRAPHY, ContentLanguage.UNKNOWN),
        'religious': (MediaType.RELIGIOUS, ContentLanguage.MIXED),
        'others': (MediaType.MIXED, ContentLanguage.MIXED),
        'series': (MediaType.SERIES, ContentLanguage.ENGLISH),
        'series original': (MediaType.SERIES, ContentLanguage.ORIGINAL),
        'series polish': (MediaType.SERIES_POLISH, ContentLanguage.POLISH),
        'theater': (MediaType.THEATER, ContentLanguage.MIXED),
    }

    @classmethod
    def classify(cls, path: str, ext: str) -> Tuple[MediaType, ContentLanguage]:
        # Zdjęcia
        if ext in {'.jpg', '.jpeg', '.png', '.raw', '.nef', '.cr2', '.arw', '.dng', '.tif', '.tiff'}:
            return MediaType.PHOTOGRAPHY, ContentLanguage.UNKNOWN

        # Audio
        if ext in {'.mp3', '.flac', '.wav', '.aac', '.ogg', '.wma'}:
            if 'music clip' in path.lower():
                return MediaType.MUSIC_CLIP, ContentLanguage.MIXED
            return MediaType.MUSIC, ContentLanguage.MIXED

        path_parts = [p.lower() for p in Path(path).parts]

        for folder_key, (media_type, language) in cls.MAPPING.items():
            if folder_key in path_parts:
                return media_type, language

        # S01E02 / 1x02 => serial
        if FilenameParser.SEASON_EP_PATTERN.search(os.path.basename(path)):
            return MediaType.SERIES, ContentLanguage.UNKNOWN

        return MediaType.UNKNOWN, ContentLanguage.UNKNOWN


# =================== SAFE FILE OPS ===================

class SafeFileOperations:
    @staticmethod
    def safe_mediainfo_parse(path: str) -> Optional[MediaInfo]:
        """
        Bezpieczne wywołanie MediaInfo.parse:
        - próbuje parse_speed=0.25 (szybciej na NAS),
        - jeśli wersja biblioteki nie wspiera tego parametru, spada do domyślnego parse().
        """
        for attempt in range(Config.NETWORK_RETRY_ATTEMPTS):
            try:
                try:
                    # parse_speed=0.25 – szybsze na udziałach sieciowych
                    return MediaInfo.parse(path, parse_speed=0.25)
                except TypeError:
                    # starsza wersja pymediainfo – bez parametru parse_speed
                    return MediaInfo.parse(path)
            except Exception as e:
                if attempt == Config.NETWORK_RETRY_ATTEMPTS - 1:
                    logger.error(f"MediaInfo failed for {os.path.basename(path)}: {e}")
                    return None
                time.sleep(Config.NETWORK_RETRY_DELAY)
        return None


# =================== EXTRACTOR ENGINE ===================

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
        # obsługa zarówno "cd/m2" jak i "cd/m²"
        match = re.search(r'(\d+)\s*cd/m(?:2|²)', str(text))
        return int(match.group(1)) if match else None

    @staticmethod
    def _parse_delay_ms(delay_str: str) -> Optional[float]:
        if not delay_str:
            return None
        match = re.search(r'([-+]?\d+(?:\.\d+)?)\s*ms', str(delay_str))
        return float(match.group(1)) if match else None

    @staticmethod
    def _calculate_quality_score(record: MediaRecord) -> float:
        score = 0.0
        # Resolution
        if record.is_4k:
            score += 30
        elif record.height and record.height >= 1080:
            score += 20
        elif record.height and record.height >= 720:
            score += 10
        # HDR
        if record.has_dolby_vision:
            score += 20
        elif record.has_hdr:
            score += 15
        # Audio
        if record.has_atmos or record.has_dts_x:
            score += 25
        elif record.has_lossless_audio:
            score += 20
        elif record.audio_count > 1:
            score += 10
        # Bitrate
        if record.overall_bitrate_kbps:
            if record.overall_bitrate_kbps > 50000:
                score += 15
            elif record.overall_bitrate_kbps > 20000:
                score += 10
            elif record.overall_bitrate_kbps > 10000:
                score += 5
        # Extras
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
                meta['image_width'] = img.width
                meta['image_height'] = img.height
                meta['image_megapixels'] = round((img.width * img.height) / 1_000_000, 2)
                exif = img.getexif()
                if exif:
                    for tag_id, value in exif.items():
                        tag = TAGS.get(tag_id, tag_id)
                        if tag == 'Make':
                            meta['camera_make'] = str(value).strip()
                        elif tag == 'Model':
                            meta['camera_model'] = str(value).strip()
                        elif tag == 'DateTimeOriginal':
                            meta['date_taken'] = str(value).strip()
        except Exception:
            pass
        return meta

    @classmethod
    def process_file(cls, path: str, root_dir: str, file_info: Tuple[int, float, float]) -> MediaRecord:
        """Core extraction logic. Takes file stats from tuple to avoid extra I/O."""

        f_size, f_mtime, f_ctime = file_info

        filename = os.path.basename(path)
        folder = os.path.basename(os.path.dirname(path))
        ext = os.path.splitext(filename)[1].lower()

        media_type, language = MediaTypeClassifier.classify(path, ext)
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
            content_language=language.value,
            size_bytes=f_size,
            size_mb=round(f_size / (1024 * 1024), 2),
            size_gb=round(f_size / (1024 * 1024 * 1024), 3),
            created_at_ts=datetime.fromtimestamp(f_ctime).isoformat(),
            modified_at_ts=datetime.fromtimestamp(f_mtime).isoformat(),
            scanned_at_ts=datetime.now().isoformat(),
            extracted_year=year,
            extracted_title=clean_title,
            season_number=season,
            episode_number=episode,
            parsed_resolution=quality['resolution'],
            parsed_source=quality['source'],
            is_polish_production=1 if language == ContentLanguage.POLISH else 0,
        )

        # Image-only file
        if media_type == MediaType.PHOTOGRAPHY:
            img_meta = cls.extract_image_metadata(path)
            record.image_width = img_meta.get('image_width')
            record.image_height = img_meta.get('image_height')
            record.image_megapixels = img_meta.get('image_megapixels')
            record.camera_make = img_meta.get('camera_make')
            record.camera_model = img_meta.get('camera_model')
            record.date_taken = img_meta.get('date_taken')
            return record

        # Video/Audio
        try:
            mi = SafeFileOperations.safe_mediainfo_parse(path)
            if not mi:
                record.processing_error = "MediaInfo parse failed"
                return record

            # Temporary collectors
            audio_langs, audio_formats, audio_commercials = [], [], []
            audio_bitrates, audio_channels, audio_titles = [], [], []
            audio_delays, audio_service_kinds = [], []

            subtitle_langs, subtitle_formats, subtitle_titles = [], [], []
            subtitle_events, subtitle_muxing = [], []

            for track in mi.tracks:

                # --- GENERAL TRACK ---
                if track.track_type == 'General':
                    record.container_format = getattr(track, 'format', None)
                    record.format_version = getattr(track, 'format_version', None)
                    record.writing_application = getattr(track, 'writing_application', None)
                    record.encoded_date = getattr(track, 'encoded_date', None)
                    record.movie_name = getattr(track, 'movie_name', None)

                    # Series info
                    record.collection_name = getattr(track, 'collection', None)
                    record.track_name = getattr(track, 'track_name', None)
                    record.performer = getattr(track, 'performer', None)
                    record.content_description = getattr(track, 'description', None)
                    record.recorded_date = getattr(track, 'recorded_date', None)
                    record.part_id = cls._safe_int(getattr(track, 'part_id', None))

                    # AI & Extras
                    videoai = getattr(track, 'videoai', None)
                    if videoai:
                        record.has_ai_upscaling = 1
                        record.ai_processing_info = str(videoai)[:300]

                    record.error_detection_type = getattr(track, 'errordetectiontype', None)
                    attach = getattr(track, 'attachments', None)
                    if attach:
                        record.has_attachments = 1
                        record.has_cover_art = 1
                        record.attachment_list = str(attach)

                    uid = getattr(track, 'unique_id', None)
                    if uid:
                        record.matroska_unique_id = str(uid)

                    dur_ms = cls._safe_float(getattr(track, 'duration', None))
                    if dur_ms:
                        record.duration_sec = round(dur_ms / 1000, 2)
                        record.duration_fmt = time.strftime('%H:%M:%S', time.gmtime(record.duration_sec))

                    obr = cls._safe_float(getattr(track, 'overall_bit_rate', None))
                    if obr:
                        record.overall_bitrate_kbps = round(obr / 1000, 2)

                # --- VIDEO TRACK ---
                elif track.track_type == 'Video':
                    record.video_codec = getattr(track, 'format', None)
                    record.video_profile = getattr(track, 'format_profile', None)
                    record.writing_library = getattr(track, 'writing_library', None)

                    enc_settings = getattr(track, 'encoding_settings', None)
                    if enc_settings:
                        record.encoding_settings = str(enc_settings)[:500]

                    w = cls._safe_int(getattr(track, 'width', None))
                    h = cls._safe_int(getattr(track, 'height', None))
                    if w and h:
                        record.width, record.height = w, h
                        record.video_resolution = f"{w}x{h}"
                        record.is_4k = 1 if (w >= 3800 or h >= 2100) else 0

                    dar = getattr(track, 'display_aspect_ratio', None)
                    if dar:
                        record.aspect_ratio = str(dar)

                    record.frame_rate = cls._safe_float(getattr(track, 'frame_rate', None))
                    vbr = cls._safe_float(getattr(track, 'bit_rate', None))
                    if vbr:
                        record.video_bitrate_kbps = round(vbr / 1000, 2)
                    vbr_max = cls._safe_float(getattr(track, 'bit_rate_maximum', None))
                    if vbr_max:
                        record.video_bit_rate_max_kbps = round(vbr_max / 1000, 2)

                    src_dur = cls._safe_float(getattr(track, 'source_duration', None))
                    if src_dur:
                        record.source_duration_sec = round(src_dur / 1000, 2)

                    record.timecode_first_frame = getattr(track, 'time_code_of_first_frame', None)
                    record.video_bit_depth = cls._safe_int(getattr(track, 'bit_depth', None))
                    record.scan_type = getattr(track, 'scan_type', None)
                    record.color_space = getattr(track, 'color_space', None)

                    # HDR Metadata
                    record.hdr_format_commercial = getattr(track, 'hdr_format', None)
                    record.color_primaries = getattr(track, 'color_primaries', None)
                    record.transfer_characteristics = getattr(track, 'transfer_characteristics', None)
                    record.matrix_coefficients = getattr(track, 'matrix_coefficients', None)
                    record.mastering_display_primaries = getattr(track, 'mastering_display_color_primaries', None)

                    lum = getattr(track, 'mastering_display_luminance', None)
                    if lum:
                        record.mastering_display_luminance = str(lum)

                    max_cll = getattr(track, 'maximum_content_light_level', None)
                    if max_cll:
                        record.max_content_light_level = cls._extract_light_level(str(max_cll))

                    max_fall = getattr(track, 'maximum_frameaverage_light_level', None)
                    if max_fall:
                        record.max_frame_avg_light_level = cls._extract_light_level(str(max_fall))

                    # HDR Flags
                    hdr_fmt = str(record.hdr_format_commercial or '').lower()
                    if 'dolby' in hdr_fmt or 'vision' in hdr_fmt:
                        record.has_dolby_vision = 1
                        record.has_hdr = 1
                    elif hdr_fmt or (record.video_bit_depth and record.video_bit_depth >= 10):
                        record.has_hdr = 1

                # --- AUDIO TRACK ---
                elif track.track_type == 'Audio':
                    record.audio_count += 1
                    lang = getattr(track, 'language', 'und')
                    fmt = getattr(track, 'format', None)
                    commercial = getattr(track, 'commercial_name', None)
                    channels = getattr(track, 'channel_s', None)
                    title = getattr(track, 'title', None)
                    service_kind = getattr(track, 'service_kind', None)

                    delay = getattr(track, 'delay_relative_to_video', None)
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

                    abr = cls._safe_float(getattr(track, 'bit_rate', None))
                    if abr:
                        audio_bitrates.append(str(int(abr / 1000)))

                    if lang and lang.lower() in ['pl', 'pol', 'polish']:
                        record.has_polish_audio = 1

                    # Main track info
                    if record.audio_count == 1:
                        record.audio_main_codec = fmt
                        record.audio_main_language = lang
                        record.audio_main_channels = str(channels) if channels else None
                        if abr:
                            record.audio_main_bitrate_kbps = round(abr / 1000, 2)

                    # Flags
                    if commercial:
                        comm_lower = commercial.lower()
                        if 'atmos' in comm_lower:
                            record.has_atmos = 1
                        if 'dts:x' in comm_lower or 'dts-x' in comm_lower:
                            record.has_dts_x = 1

                    comp = getattr(track, 'compression_mode', '')
                    if 'lossless' in str(comp).lower():
                        record.has_lossless_audio = 1
                    if fmt and fmt.upper() in ['FLAC', 'ALAC', 'MLP FBA', 'DTS XLL', 'TRUEHD']:
                        record.has_lossless_audio = 1

                # --- SUBTITLE TRACK ---
                elif track.track_type == 'Text':
                    record.subtitle_count += 1
                    lang = getattr(track, 'language', 'und')
                    fmt = getattr(track, 'format', None)
                    title = getattr(track, 'title', None)
                    forced = getattr(track, 'forced', None)
                    muxing = getattr(track, 'muxing_mode', None)
                    events = getattr(track, 'count_of_elements', None)

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

                    if lang and lang.lower() in ['pl', 'pol', 'polish']:
                        record.has_polish_subtitles = 1
                    if forced and str(forced).lower() == 'yes':
                        record.subtitle_forced_count += 1

                # --- MENU / CHAPTERS ---
                elif track.track_type == 'Menu':
                    record.has_chapters = 1
                    menu_data = getattr(track, 'extra', {})
                    if isinstance(menu_data, dict):
                        record.chapter_count = len([k for k in menu_data.keys() if k.startswith('00:')])

            # Post-processing lists
            record.audio_languages_list = ','.join(audio_langs) if audio_langs else None
            record.audio_formats = ','.join(audio_formats) if audio_formats else None
            record.audio_commercial_names = ','.join(audio_commercials) if audio_commercials else None
            record.audio_bitrates = ','.join(audio_bitrates) if audio_bitrates else None
            record.audio_channels_list = ','.join(audio_channels) if audio_channels else None
            record.audio_titles = ','.join(audio_titles) if audio_titles else None
            record.audio_service_kinds = ','.join(audio_service_kinds) if audio_service_kinds else None
            record.audio_delays_ms = ','.join(audio_delays) if audio_delays else None

            record.subtitle_languages_list = ','.join(subtitle_langs) if subtitle_langs else None
            record.subtitle_formats = ','.join(subtitle_formats) if subtitle_formats else None
            record.subtitle_titles = ','.join(subtitle_titles) if subtitle_titles else None
            record.subtitle_event_counts = ','.join(subtitle_events) if subtitle_events else None
            record.subtitle_muxing_modes = ','.join(subtitle_muxing) if subtitle_muxing else None

            record.is_multi_audio = 1 if record.audio_count > 1 else 0
            record.has_subtitles = 1 if record.subtitle_count > 0 else 0
            record.quality_score = cls._calculate_quality_score(record)

        except Exception as e:
            logger.error(f"Extraction error {filename}: {e}")
            record.processing_error = str(e)

        return record


# =================== TMDB ===================

class TMDBManager:
    """Rate-limited API Wrapper (thread-safe)."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'MediaScanner/3.3'})
        self.last_request_time = 0.0
        self.request_count = 0
        self.error_count = 0
        self._lock = Lock()

    def _rate_limit(self):
        with self._lock:
            elapsed = time.time() - self.last_request_time
            if elapsed < Config.TMDB_RATE_LIMIT_DELAY:
                time.sleep(Config.TMDB_RATE_LIMIT_DELAY - elapsed)
            self.last_request_time = time.time()

    def _request(self, url: str, params: Dict) -> Optional[Dict]:
        for attempt in range(Config.TMDB_MAX_RETRIES):
            try:
                self._rate_limit()
                r = self.session.get(url, params=params, timeout=Config.TMDB_REQUEST_TIMEOUT)
                with self._lock:
                    self.request_count += 1
                if r.status_code == 429:
                    time.sleep(2 ** attempt)
                    continue
                r.raise_for_status()
                return r.json()
            except requests.RequestException:
                with self._lock:
                    self.error_count += 1
                if attempt == Config.TMDB_MAX_RETRIES - 1:
                    return None
                time.sleep(1)
        return None

    def enrich(self, record: MediaRecord) -> MediaRecord:
        if not Config.ENABLE_TMDB:
            return record
        if record.media_type in [
            MediaType.PHOTOGRAPHY.value,
            MediaType.MUSIC.value,
            MediaType.MUSIC_CLIP.value,
        ]:
            return record

        is_series = (
            record.media_type
            in [MediaType.SERIES.value, MediaType.SERIES_POLISH.value, MediaType.KIDS_SERIES.value]
            or record.season_number is not None
        )
        endpoint = "tv" if is_series else "movie"

        params = {
            "api_key": Config.TMDB_API_KEY,
            "query": record.extracted_title,
            "language": "pl-PL" if record.is_polish_production else "en-US",
        }
        if record.extracted_year and not is_series:
            params["year"] = record.extracted_year

        data = self._request(f"{Config.TMDB_BASE_URL}/search/{endpoint}", params)

        if not data or not data.get('results'):
            if "year" in params:
                del params["year"]
                data = self._request(f"{Config.TMDB_BASE_URL}/search/{endpoint}", params)

        if data and data.get('results'):
            best = data['results'][0]
            record.tmdb_id = best.get('id')

            # Fetch details for genres/runtime
            details = self._request(
                f"{Config.TMDB_BASE_URL}/{endpoint}/{record.tmdb_id}",
                {"api_key": Config.TMDB_API_KEY, "language": "pl-PL"},
            )
            if details:
                record.tmdb_title = details.get('title') or details.get('name')
                record.tmdb_original_title = details.get('original_title') or details.get('original_name')
                record.tmdb_rating = details.get('vote_average')
                record.tmdb_vote_count = details.get('vote_count')
                record.tmdb_genres = ','.join([g['name'] for g in details.get('genres', [])])
                record.tmdb_release_date = (
                    details.get('release_date') or details.get('first_air_date')
                )
                record.tmdb_overview = details.get('overview')
                record.tmdb_runtime = details.get('runtime') or details.get('episode_run_time', [None])[0]
                record.tmdb_production_countries = ','.join(
                    [c.get('iso_3166_1', '') for c in details.get('production_countries', [])]
                )
                record.tmdb_poster_path = details.get('poster_path')
                record.tmdb_enriched = 1

        return record


# =================== CACHE & ERROR ===================

class CacheManager:
    def __init__(self):
        self.path = Config.CACHE_FILE
        self.lock = Lock()
        self.cache = self._load()
        self.dirty = False
        self.hits = 0
        self.misses = 0

    def _load(self):
        if not os.path.exists(self.path):
            return {"version": Config.CACHE_VERSION, "entries": {}}
        try:
            with open(self.path, 'r', encoding='utf-8') as f:
                c = json.load(f)
                return c if c.get("version") == Config.CACHE_VERSION else {"version": Config.CACHE_VERSION, "entries": {}}
        except Exception:
            return {"version": Config.CACHE_VERSION, "entries": {}}

    def save(self):
        with self.lock:
            if not self.dirty:
                return
            try:
                with open(self.path, 'w', encoding='utf-8') as f:
                    json.dump(self.cache, f, ensure_ascii=False, indent=2)
                self.dirty = False
            except Exception as e:
                logger.error(f"Cache save failed: {e}")

    def get(self, path: str, mtime: float, size: int):
        with self.lock:
            entry = self.cache["entries"].get(path)
            if entry and entry["mtime"] == mtime and entry["size"] == size:
                self.hits += 1
                return entry["data"]
            self.misses += 1
            return None

    def set(self, path: str, mtime: float, size: int, data: Dict):
        with self.lock:
            self.cache["entries"][path] = {"mtime": mtime, "size": size, "data": data}
            self.dirty = True

    def prune(self, valid: Set[str]):
        if not Config.PRUNE_CACHE_MISSING:
            return
        with self.lock:
            current = set(self.cache["entries"].keys())
            to_del = current - valid
            if to_del:
                for k in to_del:
                    del self.cache["entries"][k]
                self.dirty = True
                self.save()

    def stats(self):
        with self.lock:
            t = self.hits + self.misses
            return {"hits": self.hits, "misses": self.misses, "rate": round(self.hits / t * 100, 1) if t else 0}


class ErrorTracker:
    def __init__(self):
        self.errors = []
        self.lock = Lock()

    def add(self, path: str, error: Exception, stage: str):
        with self.lock:
            self.errors.append(
                {
                    "file": os.path.basename(path),
                    "error": str(error),
                    "stage": stage,
                    "time": datetime.now().isoformat(),
                }
            )
            logger.error(f"{stage} error for {os.path.basename(path)}: {error}")

    def save(self):
        with self.lock:
            if self.errors:
                with open(Config.ERRORS_FILE, 'w', encoding='utf-8') as f:
                    json.dump(self.errors, f, indent=2)


# =================== MAIN WORKFLOW ===================

def collect_files() -> List[Tuple[str, str, int, float, float]]:
    files: List[Tuple[str, str, int, float, float]] = []
    with console.status("[bold blue]Indexing media structure...") as status:
        for root_dir in Config.ROOT_DIRECTORIES:
            if not os.path.exists(root_dir):
                continue
            status.update(f"[bold blue]Scanning: {os.path.basename(root_dir)}")
            try:
                for root, _, filenames in os.walk(root_dir):
                    for filename in filenames:
                        ext = os.path.splitext(filename)[1].lower()
                        if ext in Config.ALLOWED_EXTENSIONS:
                            full_path = os.path.join(root, filename)
                            try:
                                s = os.stat(full_path)
                                files.append((full_path, root_dir, s.st_size, s.st_mtime, s.st_ctime))
                            except OSError:
                                pass
            except Exception:
                pass
    logger.info(f"Indexed {len(files)} files.")
    return files


def process_item(file_data, cache: CacheManager, tmdb: TMDBManager, errors: ErrorTracker):
    path, root, size, mtime, ctime = file_data
    try:
        cached = cache.get(path, mtime, size)
        needs_tmdb = (
            Config.ENABLE_TMDB
            and Config.FORCE_TMDB_IF_MISSING
            and cached
            and not cached.get('tmdb_enriched')
        )

        if cached and not needs_tmdb:
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


def main():
    console.print("[bold green]Media Scanner v3.3 (Production)[/bold green]")
    try:
        Config.validate()
    except Exception as e:
        console.print(f"[red]Config Error: {e}[/red]")
        return

    cache = CacheManager()
    tmdb = TMDBManager()
    errors = ErrorTracker()

    files = collect_files()
    if not files:
        console.print("[yellow]No files found under configured ROOT_DIRECTORIES.[/yellow]")
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
    ) as p:
        task = p.add_task("Processing...", total=len(files))

        with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
            futures = {executor.submit(process_item, f, cache, tmdb, errors): f[0] for f in files}

            for future in as_completed(futures):
                path = futures[future]
                valid_paths.add(path)
                try:
                    res = future.result()
                    if res:
                        results.append(res)
                    if len(results) % Config.CACHE_SAVE_INTERVAL == 0:
                        cache.save()
                except Exception:
                    pass
                p.update(task, advance=1)

    cache.save()
    cache.prune(valid_paths)
    errors.save()

    if results:
        df = pd.DataFrame(results)
        df.to_csv(Config.CSV_FILE, index=False, encoding='utf-8-sig', escapechar='\\')

        console.print(f"\n[bold green]Done! {len(df)} records saved to {Config.CSV_FILE}[/bold green]")
        if 'size_gb' in df.columns:
            console.print(f"Total Size: {df['size_gb'].sum():.2f} GB")
        console.print(f"Cache Hit Rate: {cache.stats()['rate']}%")

        with open(Config.STATS_FILE, 'w', encoding='utf-8') as f:
            json.dump(
                {
                    "date": datetime.now().isoformat(),
                    "total": len(df),
                    "size_gb": float(df['size_gb'].sum()) if 'size_gb' in df.columns else None,
                    "cache": cache.stats(),
                    "errors": len(errors.errors),
                },
                f,
                indent=2,
            )


# =================== DATABRICKS SCHEMA (Copy to Notebook) ===================
"""
from pyspark.sql.types import *

media_schema = StructType([
    StructField("file_path", StringType(), False),
    StructField("filename", StringType(), True),
    StructField("folder_name", StringType(), True),
    StructField("extension", StringType(), True),
    StructField("source_root", StringType(), True),
    StructField("media_type", StringType(), True),
    StructField("content_language", StringType(), True),
    StructField("size_bytes", LongType(), True),
    StructField("size_mb", DoubleType(), True),
    StructField("size_gb", DoubleType(), True),
    StructField("created_at_ts", StringType(), True),
    StructField("modified_at_ts", StringType(), True),
    StructField("scanned_at_ts", StringType(), True),
    StructField("extracted_year", IntegerType(), True),
    StructField("season_number", IntegerType(), True),
    StructField("episode_number", IntegerType(), True),
    StructField("video_resolution", StringType(), True),
    StructField("width", IntegerType(), True),
    StructField("height", IntegerType(), True),
    StructField("is_4k", IntegerType(), True),
    StructField("has_hdr", IntegerType(), True),
    StructField("has_atmos", IntegerType(), True),
    StructField("has_dts_x", IntegerType(), True),
    StructField("has_lossless_audio", IntegerType(), True),
    StructField("audio_count", IntegerType(), True),
    StructField("subtitle_count", IntegerType(), True),
    StructField("has_polish_audio", IntegerType(), True),
    StructField("has_polish_subtitles", IntegerType(), True),
    StructField("tmdb_id", IntegerType(), True),
    StructField("tmdb_title", StringType(), True),
    StructField("tmdb_rating", DoubleType(), True),
    StructField("quality_score", DoubleType(), True),
])
"""

if __name__ == "__main__":
    main()
