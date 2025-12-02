# Media Data – Advanced Media Library Scanner

## Opis projektu

**Media Data** to zaawansowany skaner biblioteki multimediów napisany w Pythonie.  
Projekt powstał z myślą o:

- automatycznym **katalogowaniu osobistej biblioteki mediów** (filmy, seriale, koncerty, muzyka, zdjęcia),
- wygodnym utrzymaniu metadanych w czasie (cache, logi, statystyki).

Skaner:

- przechodzi rekursywnie po wybranych katalogach (NAS / lokalnych),
- dla każdego pliku wideo / audio / foto wyciąga bogate metadane z **MediaInfo** i EXIF,
- wzbogaca dane o informacje z **TMDB** (tytuł, ocena, gatunki itd.),
- zapisuje wynik do **CSV** (gotowe do wczytania przez Databricks Autoloader / Pandas),
- utrzymuje **cache JSON**, aby nie przetwarzać ponownie niezmienionych plików.

Docelowa architektura:

> NAS / Dysk sieciowy → `media_scanner.py` → `media_catalog_databricks.csv` → Databricks (Autoloader / Delta Lake)

---

## Najważniejsze funkcje

- **Bogate metadane techniczne z MediaInfo**  
  - kontener, kodeki, bitrate, rozdzielczość, FPS, bit depth, HDR, kolorymetria,
  - liczba ścieżek audio, języki, formaty, bitrate, kanały,
  - napisy: języki, formaty, napisy wymuszane (forced), liczba eventów,
  - informacje o rozdziałach (chapters) i attachmentach (np. okładki).

- **Klasyfikacja zawartości**  
  - `MediaType` – film, serial, film PL, serial PL, koncert, bajki, muzyka, fotografie itd.  
  - `ContentLanguage` – język „logiczny” na podstawie struktury folderów (Movies, Movies PL, Series Polish, itp.).

- **Analiza pliku na podstawie nazwy**  
  - wyciąganie roku z nazwy (`Reksio (1973).mkv`, `Movie.2022.1080p.mkv`),
  - parsowanie sezonu i odcinka (`S01E05`, `1x07`),
  - wykrywanie jakości (`4K`, `1080p`, `720p`) i źródła (`BluRay`, `Web-DL`, `HDTV`, `DVD`).

- **Integracja z TMDB (The Movie Database)**  
  - wyszukiwanie filmu / serialu po wyczyszczonym tytule,
  - pobieranie tytułu, tytułu oryginalnego, gatunków, oceny, daty premiery, runtime,
  - obsługa seriali z pustą listą `episode_run_time` (fix na błąd `list index out of range`),
  - pola gotowe do analizy w Spark / Databricks.

- **Metadane zdjęć (EXIF)**  
  - rozdzielczość, megapiksele,
  - producent i model aparatu,
  - data wykonania zdjęcia (jeśli dostępna).

- **Jakościowe flagi i score**  
  - flagi: `is_4k`, `has_hdr`, `has_dolby_vision`, `has_atmos`, `has_dts_x`, `has_lossless_audio`,
  - flagi językowe: `has_polish_audio`, `has_polish_subtitles`, `is_polish_production`,
  - **quality_score (0–100)** – prosty wskaźnik jakości materiału (rozdzielczość, HDR, audio, bitrate).

- **Przygotowanie pod Databricks / Spark**  
  - nazwy kolumn są stabilne i „spark-friendly” (snake_case),
  - pola logiczne to **liczby całkowite 0/1**, a nie `True/False`,
  - timestamps w formacie ISO (`created_at_ts`, `modified_at_ts`, `scanned_at_ts`),
  - gotowy schemat `StructType` do użycia w PySpark.

---

## Wymagania

- Python **3.8+**
- System z dostępem do:
  - lokalnego katalogu mediów, lub
  - udziału sieciowego (NAS), np. `E:\MEDIA` albo `\\192.168.50.135\Nowy1\MEDIA`.

### Biblioteki Pythona

Wszystkie zależności są w `requirements.txt`.  
Instalacja (zalecane wirtualne środowisko):

```bash
python -m venv .venv
.\.venv\Scripts\activate    # Windows
# lub
source .venv/bin/activate   # Linux / macOS

pip install -r requirements.txt
