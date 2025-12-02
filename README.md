# Media Data – Advanced Media Library Scanner

> Python tool for scanning large personal media libraries (movies, TV shows, music, photos)  
> and exporting rich technical metadata to CSV for analysis.

Polish version: [README.pl.md](README.pl.md)

---

## Project Overview

**Media Data** is a Python-based media library scanner designed to:

- recursively scan folders on a local disk or NAS (SMB share),
- extract detailed technical metadata from video/audio/photo files using **MediaInfo** and **EXIF**,
- enrich movies and TV shows with data from **TMDB** (The Movie Database),
- export the result as a **Spark‑friendly CSV** for ingestion into **Databricks / Delta Lake**.

Typical use cases:

- building a **catalog** of a large home media collection,
- analysing **quality** (4K, HDR, Dolby Vision, Atmos, DTS:X, bitrate),
- understanding **language coverage** (audio/subtitles in PL/EN/CS/ES/DE/FR/IT),
- preparing a dataset for analytics, dashboards or experiments in Databricks.

---

## Architecture

Data flow:

```text
[Disk / NAS (e.g. E:\MEDIA)] 
    -> [Python Media Scanner (multi-threaded)] 
        -> [media_catalog_databricks.csv] 
            -> [Databricks / Spark / Delta Lake / BI]
