import os
import json
import time
import re
import pandas as pd
from pymediainfo import MediaInfo
import requests
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn, MofNCompleteColumn
from rich.text import Text

# =================== KONFIGURACJA ===================
ROOT_DIRECTORIES = [
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
    r"\\192.168.50.135\Nowy1\MEDIA\Theater"
]
ALLOWED_EXTENSIONS = [".mp4", ".mkv", ".avi", ".wmv", ".ts"]
CSV_FILE = "media_catalog.csv"
CACHE_FILE = "media_cache.json"
TMDB_API_KEY = "ce100b4678073468014fb2cb45320f6a"
TMDB_BASE_URL = "https://api.themoviedb.org/3"

class GradientBarColumn(BarColumn):
    def __init__(self, bar_width=40, gradient_colors=None, **kwargs):
        super().__init__(bar_width=bar_width, **kwargs)
        self.gradient_colors = gradient_colors or [
            "#b9f6ca", "#a5d6a7", "#81c784", "#66bb6a",
            "#4caf50", "#43a047", "#388e3c", "#2e7d32", "#1b5e20"
        ]
    def render(self, task):
        completed = task.completed / task.total if task.total else 0
        width = self.bar_width
        complete_width = int(width * completed)
        bar = Text()
        n_colors = len(self.gradient_colors)
        for i in range(complete_width):
            idx = int((i / complete_width) * (n_colors - 1)) if complete_width else 0
            bar.append("█", style=self.gradient_colors[idx])
        bar.append(" " * (width - complete_width), style="grey37")
        return bar

# =================== FUNKCJE POMOCNICZE ===================
def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)

def extract_media_info(file_path):
    media_info = MediaInfo.parse(file_path)
    info = {}
    for track in media_info.tracks:
        if track.track_type == "General":
            info["Title"] = os.path.splitext(os.path.basename(file_path))[0]
            info["Complete Path"] = file_path
            info["Folder Name"] = os.path.basename(os.path.dirname(file_path))
            info["Extension"] = os.path.splitext(file_path)[1].lower()
            info["Format"] = getattr(track, "format", None)
            info["Format Profile"] = getattr(track, "format_profile", None)
            info["Codec ID"] = getattr(track, "codec_id", None)
            size = getattr(track, "file_size", None)
            if size:
                try:
                    mb = float(size) / (1024 * 1024)
                    info["File Size (MB)"] = round(mb, 2)
                    info["File Size (GB)"] = round(mb / 1024, 2)
                except:
                    info["File Size (MB)"] = info["File Size (GB)"] = None
            else:
                info["File Size (MB)"] = info["File Size (GB)"] = None
            dur = getattr(track, "duration", None)
            info["Duration"] = dur
            info["Duration Formatted"] = (
                time.strftime('%H:%M:%S', time.gmtime(int(dur) // 1000)) if dur else None
            )
            br = getattr(track, "overall_bit_rate", None)
            if br:
                info["Overall Bitrate (kbps)"] = br
                info["Overall Bitrate (Mbps)"] = round(float(br) / 1000, 2)
            else:
                info["Overall Bitrate (kbps)"] = info["Overall Bitrate (Mbps)"] = None
            info["Recorded Date"] = getattr(track, "recorded_date", None)
            info["Writing Application"] = getattr(track, "writing_application", None)
        elif track.track_type == "Video":
            info["Video Codec"] = getattr(track, "format", None)
            vb = getattr(track, "bit_rate", None)
            if vb:
                info["Video Bitrate (kbps)"] = vb
                info["Video Bitrate (Mbps)"] = round(float(vb) / 1000, 2)
            else:
                info["Video Bitrate (kbps)"] = info["Video Bitrate (Mbps)"] = None
            w, h = getattr(track, "width", None), getattr(track, "height", None)
            info["Resolution"] = f"{w}x{h}" if w and h else None
            # rzutowania numeryczne
            try:
                info["Aspect Ratio"] = float(getattr(track, "display_aspect_ratio"))
            except:
                info["Aspect Ratio"] = None
            try:
                info["Frame Rate (FPS)"] = float(getattr(track, "frame_rate"))
            except:
                info["Frame Rate (FPS)"] = None
            # pozostałe pola
            for fld in [
                "color_space", "chroma_subsampling", "bit_depth", "scan_type",
                "writing_library", "hdr_format", "color_range",
                "color_primaries", "transfer_characteristics",
                "matrix_coefficients", "maximum_content_light_level",
                "maximum_frame_average_light_level"
            ]:
                key = ' '.join([w.capitalize() for w in fld.split('_')])
                info[key] = getattr(track, fld, None)
        elif track.track_type == "Audio":
            tr = info.setdefault("Audio Tracks", [])
            tr.append({
                "Codec": getattr(track, "format", None),
                "Bitrate (kbps)": getattr(track, "bit_rate", None),
                "Channels": getattr(track, "channel_s", None),
                "Language": getattr(track, "language", None),
                "Default": getattr(track, "default", None),
                "Forced": getattr(track, "forced", None)
            })
        elif track.track_type == "Text":
            st = info.setdefault("Subtitles", [])
            lang = getattr(track, "language", None)
            if lang:
                st.append(lang)
    # spłaszczenie audio
    tracks = info.get("Audio Tracks", [])
    info["Number of Audio Tracks"] = len(tracks)
    for i, t in enumerate(tracks, 1):
        for k, v in t.items():
            info[f"Audio Track {i} {k}"] = v
    info["Subtitles Info"] = ", ".join(info.get("Subtitles", []))
    return info

def query_tmdb(title, is_series=False):
    url = f"{TMDB_BASE_URL}/search/{'tv' if is_series else 'movie'}"
    year_match = re.search(r"\((\d{4})\)", title)
    params = {"api_key": TMDB_API_KEY, "query": title, "language": "en-US"}
    if year_match and not is_series:
        params["year"] = year_match.group(1)
    try:
        r = requests.get(url, params=params).json()
        if r.get("results"): return r["results"][0]
        cleaned = re.sub(r"\s*\(\d{4}\)", "", title)
        if cleaned != title:
            params["query"] = cleaned
            params.pop("year", None)
            r = requests.get(url, params=params).json()
            if r.get("results"): return r["results"][0]
    except:
        pass
    return None

def get_tmdb_polish_details(tid, is_series=False):
    url = f"{TMDB_BASE_URL}/{'tv' if is_series else 'movie'}/{tid}"
    try:
        return requests.get(url, params={"api_key": TMDB_API_KEY, "language": "pl-PL"}).json()
    except:
        return None

def enrich_with_tmdb(info):
    is_series = 'series' in info.get("Folder Name", "").lower()
    data = query_tmdb(info.get("Title"), is_series)
    if data:
        details = get_tmdb_polish_details(data.get("id"), is_series)
        if details:
            info["TMDB Genre(s)"] = ", ".join([g.get("name", "") for g in details.get("genres", [])])
            info["TMDB Rating"] = details.get("vote_average")
            polish = details.get("name") if is_series else details.get("title")
            info["Polish Title"] = polish if polish and polish != info.get("Title") else None
        else:
            info.update({"TMDB Genre(s)": None, "TMDB Rating": None, "Polish Title": None})
    else:
        info.update({"TMDB Genre(s)": None, "TMDB Rating": None, "Polish Title": None})
    return info

def process_file(path, cache):
    m = os.path.getmtime(path)
    if path in cache and cache[path]["modified"] == m:
        return cache[path]["metadata"]
    info = extract_media_info(path)
    info = enrich_with_tmdb(info)
    cache[path] = {"modified": m, "metadata": info}
    return info

def scan_media_files():
    media = []
    cache = load_cache()
    dirs = ROOT_DIRECTORIES if isinstance(ROOT_DIRECTORIES, list) else [ROOT_DIRECTORIES]
    files = [
        (os.path.join(r, f), d)
        for d in dirs
        for r, _, fs in os.walk(d)
        for f in fs
        if os.path.splitext(f)[1].lower() in ALLOWED_EXTENSIONS
    ]
    with Progress(
        TextColumn("[bold blue]{task.description}"),
        GradientBarColumn(bar_width=40),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn()
    ) as prog:
        task = prog.add_task("Przetwarzanie plików...", total=len(files))
        for p, d in files:
            try:
                fi = process_file(p, cache)
                fi["Source Directory"] = os.path.basename(d)
                media.append(fi)
            except Exception as e:
                print(f"Error processing {p}: {e}")
            prog.update(task, advance=1)
    save_cache(cache)
    return media

def update_csv(media_data):
    df_new = pd.DataFrame(media_data)
    if os.path.exists(CSV_FILE):
        df_exist = pd.read_csv(CSV_FILE)
        df_exist.set_index("Complete Path", inplace=True)
        df_new.set_index("Complete Path", inplace=True)
        # konwersja wszystkich numerycznych kolumn na numeric
        num_cols = df_exist.select_dtypes(include=['number']).columns
        for col in num_cols:
            if col in df_new.columns:
                df_new[col] = pd.to_numeric(df_new[col], errors="coerce")
        df_exist.update(df_new)
        df_combined = pd.concat([
            df_exist,
            df_new[~df_new.index.isin(df_exist.index)]
        ])
        df_combined.reset_index(inplace=True)
    else:
        df_combined = df_new
    # zachowanie kolejności kolumn
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
        "Subtitles Info", "TMDB Genre(s)", "TMDB Rating", "Polish Title"
    ]
    cols = [c for c in desired if c in df_combined.columns]
    df_combined[cols].to_csv(CSV_FILE, index=False)
    print(f"CSV zaktualizowany: {CSV_FILE}")

def main():
    data = scan_media_files()
    update_csv(data)

if __name__ == "__main__":
    main()
