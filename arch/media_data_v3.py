import os
import re
import json
import time
import math
from typing import Any, Dict, List, Optional, Tuple, Set

import pandas as pd
import requests
from pymediainfo import MediaInfo
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    MofNCompleteColumn,
)
from rich.text import Text

# NEW: wczytaj zmienne z .env (jeśli plik jest w katalogu projektu)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    # jeśli nie zainstalowano python-dotenv, po prostu pomiń (VS Code może sam załadować .env)
    pass

# =================== KONFIGURACJA ===================

ROOT_DIRECTORIES: List[str] = [
    r"\\192.168.50.135\Nowy1\MEDIA\Christmas",
    r"\\192.168.50.135\Nowy1\MEDIA\Concert",
    r"\\192.168.50.135\Nowy1\MEDIA\Docu",
    r"\\192.168.50.135\Nowy1\MEDIA\Kids Movies",
    r"\\192.168.50.135\Nowy1\MEDIA\Kids Series",
    r"\\192.168.50.135\Nowy1\MEDIA\Movies",
    r"\\192.168.50.135\Nowy1\MEDIA\Movies PL",
    r"\\192.168.50.135\Nowy1\MEDIA\Music",
    r"\\192.168.50.135\Nowy1\MEDIA\Music Clips",
    r"\\192.168.50.135\Nowy1\MEDIA\Series",
    r"\\192.168.50.135\Nowy1\MEDIA\Series Original",
    r"\\192.168.50.135\Nowy1\MEDIA\Theater",
]

ALLOWED_EXTENSIONS = {".mp4", ".mkv", ".avi", ".wmv", ".ts"}

CSV_FILE = "media_catalog.csv"
CACHE_FILE = "media_cache.json"

TMDB_BASE_URL = "https://api.themoviedb.org/3"
# NEW: bez żadnego fallbacka w kodzie – tylko ze środowiska
TMDB_API_KEY = os.getenv("TMDB_API_KEY", "").strip()

# Flagi zachowania:
ENABLE_TMDB = True                 # włącz/wyłącz wzbogacanie TMDB (zostanie auto-wyłączone, jeśli brak klucza)
FORCE_TMDB_IF_MISSING = True       # "dograj TMDB jeśli brak" nawet przy odczycie z cache
PRUNE_CSV_MISSING = True           # usuń z CSV wpisy nieobecne w aktualnym skanie
PRUNE_CACHE_MISSING = True         # usuń z cache wpisy nieobecne w aktualnym skanie

# NEW: kontrola retry i logów dla TMDB
TMDB_DEBUG = False                 # ustaw True, by zobaczyć szczegółowe logi TMDB
TMDB_MAX_RETRIES = 3               # ile razy ponawiać 5xx/429 i błędy sieci
REQUEST_TIMEOUT = (5, 15)          # (connect timeout, read timeout) w sekundach

# NEW: jeśli nie ma klucza – wyłącz TMDB i daj czytelny komunikat (bez przerywania programu)
if ENABLE_TMDB and not TMDB_API_KEY:
    print("Uwaga: brak TMDB_API_KEY w środowisku (.env). Wzbogacanie TMDB zostaje wyłączone.")
    ENABLE_TMDB = False

# =================== WSPARCIE UI (progress bar) ===================

class GradientBarColumn(BarColumn):
    def __init__(self, bar_width: int = 40, gradient_colors: Optional[List[str]] = None, **kwargs: Any):
        super().__init__(bar_width=bar_width, **kwargs)
        self.gradient_colors = gradient_colors or [
            "#b9f6ca", "#a5d6a7", "#81c784", "#66bb6a",
            "#4caf50", "#43a047", "#388e3c", "#2e7d32", "#1b5e20"
        ]

    def render(self, task):  # type: ignore[override]
        completed = float(task.completed) / float(task.total) if task.total else 0.0
        width = self.bar_width
        complete_width = int(width * completed)
        bar = Text()
        n_colors = len(self.gradient_colors) or 1
        for i in range(complete_width):
            idx = int((i / max(complete_width, 1)) * (n_colors - 1))
            bar.append("█", style=self.gradient_colors[idx])
        bar.append(" " * (width - complete_width), style="grey37")
        return bar

# =================== FUNKCJE POMOCNICZE ===================

def load_cache() -> Dict[str, Any]:
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"Uwaga: nie udało się wczytać cache ({e}). Tworzę pusty.")
    return {}

def save_cache(cache: Dict[str, Any]) -> None:
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Uwaga: nie udało się zapisać cache ({e}).")

def _to_float_maybe(val: Any) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val)
    s = re.sub(r"[^0-9\.,]", "", s).replace(",", ".")
    try:
        return float(s) if s else None
    except ValueError:
        return None

def _to_int_maybe(val: Any) -> Optional[int]:
    f = _to_float_maybe(val)
    return int(f) if f is not None and not math.isnan(f) else None

def _parse_aspect_ratio(val: Any) -> Optional[float]:
    if val is None:
        return None
    s = str(val)
    if ":" in s:
        try:
            w, h = s.split(":")
            return float(w) / float(h)
        except Exception:
            return None
    return _to_float_maybe(s)

def _parse_fps(val: Any) -> Optional[float]:
    if val is None:
        return None
    s = str(val)
    m = re.search(r"[0-9]+(?:\.[0-9]+)?", s)
    return float(m.group(0)) if m else _to_float_maybe(s)

# --- Heurystyka: czy to serial? i czyszczenie tytułu pod TMDB ---

_SERIES_HINTS = ("series", "serial", "season", "sezon")
_EP_PATTERNS = [
    r"\bS\d{1,2}E\d{1,3}\b",        # S02E62
    r"\bSezon\s*\d+\b",             # Sezon 2
    r"\bSeason\s*\d+\b",            # Season 2
    r"\bOdcinek\s*\d+\b",           # Odcinek 3
    r"\bEpisode\s*\d+\b",           # Episode 3
    r"\bEp\s*\d+\b",                # Ep 3
]

def _guess_is_series(info: Dict[str, Any]) -> bool:
    title = (info.get("Title") or "")
    path = (info.get("Complete Path") or "")

    # 1) wzorce odcinków w nazwie pliku
    if any(re.search(pat, title, flags=re.I) for pat in _EP_PATTERNS):
        return True

    # 2) podpowiedzi w ścieżce (gdziekolwiek w drzewie folderów)
    parts = [p.strip().lower() for p in os.path.normpath(path).split(os.sep) if p.strip()]
    if any(any(h in part for h in _SERIES_HINTS) for part in parts):
        return True

    return False

def _clean_title_for_tmdb(title: str) -> str:
    t = title

    # usuń wzorce odcinków/ sezonów
    t = re.sub(r"\s*-\s*S\d{1,2}E\d{1,3}\b", "", t, flags=re.I)  # np. " - S02E62"
    for pat in _EP_PATTERNS:
        t = re.sub(pat, "", t, flags=re.I)

    # usuń nadmiarowe znaki
    t = re.sub(r"[_\.]+", " ", t)       # zamień _ i . na spacje
    t = re.sub(r"\s{2,}", " ", t)       # zbij wielokrotne spacje
    return t.strip()

def extract_media_info(file_path: str) -> Dict[str, Any]:
    media_info = MediaInfo.parse(file_path)
    info: Dict[str, Any] = {}

    for track in media_info.tracks:
        if track.track_type == "General":
            info["Title"] = os.path.splitext(os.path.basename(file_path))[0]
            info["Complete Path"] = file_path
            info["Folder Name"] = os.path.basename(os.path.dirname(file_path))
            info["Extension"] = os.path.splitext(file_path)[1].lower()

            info["Format"] = getattr(track, "format", None)
            info["Format Profile"] = getattr(track, "format_profile", None)
            info["Codec ID"] = getattr(track, "codec_id", None)

            size = _to_float_maybe(getattr(track, "file_size", None))
            if size:
                mb = size / (1024 * 1024)
                info["File Size (MB)"] = round(mb, 2)
                info["File Size (GB)"] = round(mb / 1024, 2)
            else:
                info["File Size (MB)"] = info["File Size (GB)"] = None

            dur_ms = _to_int_maybe(getattr(track, "duration", None))
            info["Duration"] = dur_ms
            info["Duration Formatted"] = (
                time.strftime("%H:%M:%S", time.gmtime(int(dur_ms / 1000))) if dur_ms else None
            )

            br_overall = _to_float_maybe(getattr(track, "overall_bit_rate", None))
            if br_overall:
                info["Overall Bitrate (kbps)"] = round(br_overall / 1000, 2)
                info["Overall Bitrate (Mbps)"] = round(br_overall / 1_000_000, 3)
            else:
                info["Overall Bitrate (kbps)"] = info["Overall Bitrate (Mbps)"] = None

            info["Recorded Date"] = getattr(track, "recorded_date", None)
            info["Writing Application"] = getattr(track, "writing_application", None)

        elif track.track_type == "Video":
            info["Video Codec"] = getattr(track, "format", None)

            vbr = _to_float_maybe(getattr(track, "bit_rate", None))
            if vbr:
                info["Video Bitrate (kbps)"] = round(vbr / 1000, 2)
                info["Video Bitrate (Mbps)"] = round(vbr / 1_000_000, 3)
            else:
                info["Video Bitrate (kbps)"] = info["Video Bitrate (Mbps)"] = None

            w = _to_int_maybe(getattr(track, "width", None))
            h = _to_int_maybe(getattr(track, "height", None))
            info["Resolution"] = f"{w}x{h}" if w and h else None

            info["Aspect Ratio"] = _parse_aspect_ratio(getattr(track, "display_aspect_ratio", None))
            info["Frame Rate (FPS)"] = _parse_fps(getattr(track, "frame_rate", None))

            mapping: List[Tuple[str, str]] = [
                ("color_space", "Color Space"),
                ("chroma_subsampling", "Chroma Subsampling"),
                ("bit_depth", "Video Bit Depth"),
                ("scan_type", "Scan Type"),
                ("writing_library", "Encoding Library"),
                ("hdr_format", "HDR Format"),
                ("color_range", "Color Range"),
                ("color_primaries", "Color Primaries"),
                ("transfer_characteristics", "Transfer Characteristics"),
                ("matrix_coefficients", "Matrix Coefficients"),
                ("maximum_content_light_level", "Maximum Content Light Level (cd/m²)"),
                ("maximum_frame_average_light_level", "Maximum Frame-Average Light Level (cd/m²)"),
            ]
            for attr, key in mapping:
                info[key] = getattr(track, attr, None)

        elif track.track_type == "Audio":
            tracks = info.setdefault("Audio Tracks", [])
            channels = getattr(track, "channel_s", None) or getattr(track, "channels", None)
            if channels is None:
                other = getattr(track, "other_channel_s", None)
                if isinstance(other, list) and other:
                    channels = other[0]

            tracks.append(
                {
                    "Codec": getattr(track, "format", None),
                    "Bitrate (kbps)": _to_float_maybe(getattr(track, "bit_rate", None)),
                    "Channels": channels,
                    "Language": getattr(track, "language", None),
                    "Default": getattr(track, "default", None),
                    "Forced": getattr(track, "forced", None),
                }
            )

        elif track.track_type == "Text":
            subs = info.setdefault("Subtitles", [])
            lang = getattr(track, "language", None)
            if lang:
                subs.append(lang)

    tracks = info.get("Audio Tracks", [])
    info["Number of Audio Tracks"] = len(tracks)
    for i, t in enumerate(tracks, 1):
        for k, v in t.items():
            info[f"Audio Track {i} {k}"] = v

    subs = list(dict.fromkeys(info.get("Subtitles", [])))
    info["Subtitles Info"] = ", ".join(subs)

    return info

# =================== TMDB ===================

def _http_get(url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """GET z retry dla 5xx/429 i błędów sieci. Minimalne logi, chyba że TMDB_DEBUG=True."""
    last_status: Optional[int] = None
    for attempt in range(1, TMDB_MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            last_status = r.status_code

            # 5xx – błąd po stronie TMDB: spróbuj ponownie
            if 500 <= r.status_code < 600:
                if TMDB_DEBUG:
                    print(f"TMDB 5xx (attempt {attempt}/{TMDB_MAX_RETRIES}): {r.status_code} for {url}")
                time.sleep(1.5 * attempt)
                continue

            # 429 – ograniczenie zapytań
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", "2") or "2")
                if TMDB_DEBUG:
                    print(f"TMDB 429: retry after {wait}s")
                time.sleep(max(wait, 2) * attempt)
                continue

            r.raise_for_status()
            return r.json()

        except requests.exceptions.RequestException as e:
            if TMDB_DEBUG:
                print(f"TMDB request error (attempt {attempt}/{TMDB_MAX_RETRIES}): {e}")
            time.sleep(1.5 * attempt)
            continue

    if TMDB_DEBUG:
        print(f"TMDB: giving up after {TMDB_MAX_RETRIES} attempts (last HTTP {last_status})")
    return None

def query_tmdb(title: str, is_series: bool = False) -> Optional[Dict[str, Any]]:
    if not ENABLE_TMDB or not TMDB_API_KEY:
        return None

    url = f"{TMDB_BASE_URL}/search/{'tv' if is_series else 'movie'}"
    params = {"api_key": TMDB_API_KEY, "query": title, "language": "en-US"}

    year_match = re.search(r"\((\d{4})\)", title)
    if year_match and not is_series:
        params["year"] = year_match.group(1)

    data = _http_get(url, params=params)
    if data and data.get("results"):
        return data["results"][0]

    cleaned = re.sub(r"\s*\(\d{4}\)", "", title)
    if cleaned != title:
        params.pop("year", None)
        params["query"] = cleaned
        data = _http_get(url, params=params)
        if data and data.get("results"):
            return data["results"][0]

    return None

def get_tmdb_polish_details(tid: int, is_series: bool = False) -> Optional[Dict[str, Any]]:
    if not ENABLE_TMDB or not TMDB_API_KEY:
        return None
    url = f"{TMDB_BASE_URL}/{'tv' if is_series else 'movie'}/{tid}"
    params = {"api_key": TMDB_API_KEY, "language": "pl-PL"}
    return _http_get(url, params)

def has_tmdb(md: Dict[str, Any]) -> bool:
    return any(md.get(k) not in (None, "", []) for k in ("TMDB Genre(s)", "TMDB Rating", "Polish Title"))

def enrich_with_tmdb(info: Dict[str, Any]) -> Dict[str, Any]:
    if not ENABLE_TMDB:
        info.update({"TMDB Genre(s)": None, "TMDB Rating": None, "Polish Title": None})
        return info

    # NEW: lepsza heurystyka + czyszczenie tytułu
    is_series = _guess_is_series(info)
    query_title = _clean_title_for_tmdb(info.get("Title", ""))

    base = query_tmdb(query_title, is_series)
    if not base:
        info.update({"TMDB Genre(s)": None, "TMDB Rating": None, "Polish Title": None})
        return info

    details = get_tmdb_polish_details(base.get("id"), is_series)
    if not details:
        info.update({"TMDB Genre(s)": None, "TMDB Rating": None, "Polish Title": None})
        return info

    info["TMDB Genre(s)"] = ", ".join([g.get("name", "") for g in details.get("genres", [])])
    info["TMDB Rating"] = details.get("vote_average")
    polish = details.get("name") if is_series else details.get("title")
    info["Polish Title"] = polish if polish and polish != info.get("Title") else None
    return info

# =================== PIPELINE ===================

def process_file(path: str, cache: Dict[str, Any]) -> Dict[str, Any]:
    mtime = os.path.getmtime(path)
    fsize = os.path.getsize(path)

    # Obsługa starego cache bez "size"
    if path in cache:
        cached = cache[path]
        cached_m = cached.get("modified")
        cached_s = cached.get("size")
        if cached_m == mtime and (cached_s is None or cached_s == fsize):
            md = cached["metadata"]
            # DOGRAJ TMDB JEŚLI BRAK
            if ENABLE_TMDB and FORCE_TMDB_IF_MISSING and not has_tmdb(md):
                md = enrich_with_tmdb(md)
                cache[path] = {"modified": mtime, "size": fsize, "metadata": md}
            return md

    info = extract_media_info(path)
    info = enrich_with_tmdb(info)  # jeśli ENABLE_TMDB=False, wstawi stabilizujące None-y

    cache[path] = {"modified": mtime, "size": fsize, "metadata": info}
    return info

def _iter_media_files(dirs: List[str]) -> List[Tuple[str, str]]:
    files: List[Tuple[str, str]] = []
    for top in dirs:
        if not os.path.exists(top):
            print(f"Uwaga: katalog nie istnieje lub jest niedostępny: {top}")
            continue
        try:
            for root, _, fs in os.walk(top):
                for name in fs:
                    ext = os.path.splitext(name)[1].lower()
                    if ext in ALLOWED_EXTENSIONS:
                        files.append((os.path.join(root, name), top))
        except Exception as e:
            print(f"Uwaga: nie mogę przejść katalogu {top}: {e}")
    return files

def scan_media_files() -> Tuple[List[Dict[str, Any]], Set[str]]:
    media: List[Dict[str, Any]] = []
    cache = load_cache()

    dirs = ROOT_DIRECTORIES if isinstance(ROOT_DIRECTORIES, list) else [ROOT_DIRECTORIES]
    files = _iter_media_files(dirs)

    if not files:
        print("Brak plików do przetworzenia (sprawdź ścieżki i rozszerzenia).")
        return media, set()

    processed = 0
    current_paths: Set[str] = set()

    try:
        with Progress(
            TextColumn("[bold blue]{task.description}"),
            GradientBarColumn(bar_width=40),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
        ) as prog:
            task = prog.add_task("Przetwarzanie plików...", total=len(files))
            for path, top_dir in files:
                try:
                    md = process_file(path, cache)
                    md["Source Directory"] = os.path.basename(top_dir)
                    media.append(md)
                    current_paths.add(md["Complete Path"])
                except Exception as e:
                    print(f"Error processing {path}: {e}")
                finally:
                    processed += 1
                    prog.update(task, advance=1)
                    if processed % 50 == 0:
                        save_cache(cache)
    except KeyboardInterrupt:
        print("\nPrzerwano przez użytkownika. Kończę bieżącą iterację...")
    finally:
        save_cache(cache)

    return media, current_paths

def update_csv(media_data: List[Dict[str, Any]]) -> None:
    if not media_data:
        print("Brak danych – nie zapisuję CSV.")
        return

    df_new = pd.DataFrame(media_data)

    if "Complete Path" not in df_new.columns:
        raise RuntimeError("W danych wejściowych brakuje kolumny 'Complete Path' – nie mogę złączyć CSV.")

    df_new = df_new.drop_duplicates(subset=["Complete Path"], keep="last")

    if os.path.exists(CSV_FILE):
        try:
            df_exist = pd.read_csv(CSV_FILE)
        except Exception as e:
            print(f"Uwaga: nie mogę wczytać istniejącego CSV ({e}). Nadpisuję nowym.")
            df_exist = pd.DataFrame(columns=["Complete Path"])

        if "Complete Path" not in df_exist.columns:
            df_exist = pd.DataFrame(columns=["Complete Path"])

        df_exist = df_exist.drop_duplicates(subset=["Complete Path"], keep="last")
        df_exist.set_index("Complete Path", inplace=True)
        df_new.set_index("Complete Path", inplace=True)

        all_cols = sorted(set(df_exist.columns) | set(df_new.columns))
        df_exist = df_exist.reindex(columns=all_cols)
        df_new = df_new.reindex(columns=all_cols)

        df_exist.update(df_new)
        df_combined = pd.concat([df_exist, df_new[~df_new.index.isin(df_exist.index)]], axis=0)
        df_combined.reset_index(inplace=True)
    else:
        df_combined = df_new.reset_index()

    desired = [
        "Title", "Complete Path", "Source Directory", "Folder Name", "Extension",
        "Format", "Format Profile", "Codec ID", "File Size (MB)", "File Size (GB)",
        "Duration", "Duration Formatted", "Overall Bitrate (kbps)", "Overall Bitrate (Mbps)",
        "Recorded Date", "Writing Application", "Video Codec", "Video Bitrate (kbps)",
        "Video Bitrate (Mbps)", "Resolution", "Aspect Ratio", "Frame Rate (FPS)",
        "Color Space", "Chroma Subsampling", "Video Bit Depth", "Scan Type",
        "Encoding Library", "HDR Format", "Color Range", "Color Primaries",
        "Transfer Characteristics", "Matrix Coefficients", "Maximum Content Light Level (cd/m²)",
        "Maximum Frame-Average Light Level (cd/m²)", "Number of Audio Tracks",
        "Audio Track 1 Codec", "Audio Track 1 Bitrate (kbps)", "Audio Track 1 Channels",
        "Audio Track 1 Language", "Audio Track 1 Default", "Audio Track 1 Forced",
        "Audio Track 2 Codec", "Audio Track 2 Bitrate (kbps)", "Audio Track 2 Channels",
        "Audio Track 2 Language", "Audio Track 2 Default", "Audio Track 2 Forced",
        "Audio Track 3 Codec", "Audio Track 3 Bitrate (kbps)", "Audio Track 3 Channels",
        "Audio Track 3 Language", "Audio Track 3 Default", "Audio Track 3 Forced",
        "Subtitles Info", "TMDB Genre(s)", "TMDB Rating", "Polish Title",
    ]
    ordered = [c for c in df_combined.columns if c in desired]
    tail = [c for c in df_combined.columns if c not in ordered]
    df_final = df_combined[ordered + tail]

    df_final.to_csv(CSV_FILE, index=False, encoding="utf-8-sig")
    print(f"CSV zaktualizowany: {CSV_FILE} (rekordów: {len(df_final)})")

def prune_csv(valid_paths: Set[str]) -> None:
    if not PRUNE_CSV_MISSING:
        return
    if not os.path.exists(CSV_FILE):
        return

    try:
        df = pd.read_csv(CSV_FILE)
    except Exception as e:
        print(f"Prune CSV: nie mogę wczytać CSV ({e}). Pomijam.")
        return

    if "Complete Path" not in df.columns:
        print("Prune CSV: brak kolumny 'Complete Path'. Pomijam.")
        return

    before = len(df)
    df = df[df["Complete Path"].isin(valid_paths)]
    removed = before - len(df)
    if removed > 0:
        df.to_csv(CSV_FILE, index=False, encoding="utf-8-sig")
        print(f"Prune CSV: usunięto {removed} rekordów nieistniejących plików.")
    else:
        print("Prune CSV: nic do usunięcia.")

def prune_cache(valid_paths: Set[str]) -> None:
    if not PRUNE_CACHE_MISSING:
        return

    cache = load_cache()
    if not cache:
        return

    keys = set(cache.keys())
    to_remove = keys - valid_paths
    if not to_remove:
        print("Prune cache: nic do usunięcia.")
        return

    for k in to_remove:
        cache.pop(k, None)

    save_cache(cache)
    print(f"Prune cache: usunięto {len(to_remove)} wpisów.")

def main() -> None:
    media, current_paths = scan_media_files()
    update_csv(media)
    prune_csv(current_paths)
    prune_cache(current_paths)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nZatrzymano przez użytkownika.")
