# The Digital Pulpit Intelligence Engine v5.2

## Overview
A production-grade Python engine that converts global sermon data into theological intelligence. Operates in three phases: The Vacuum (Ingest + Transcribe), The Brain (Analysis + Drift), and The Assembly (Creative Output). Dashboard powered by Streamlit.

## Project Structure
```
├── main.py                      # Entry point — launches Streamlit on port 5000
├── streamlit_app.py             # Dashboard UI (password protected)
├── schema.sql                   # SQLite schema definition
├── engine/
│   ├── config.py                # Configuration loader (env vars, CSV, JSON)
│   ├── db.py                    # Database layer (SQLite)
│   ├── youtube.py               # YouTube API channel resolution & video discovery
│   ├── transcription.py         # Audio download (yt-dlp) & OpenAI transcription
│   ├── vacuum.py                # Vacuum orchestrator (ingest pipeline)
│   ├── brain.py                 # Theological analysis & drift metrics
│   └── assembly.py              # Avatar script generation
├── data/
│   ├── channels.csv             # Input: YouTube channels to monitor
│   └── digital_pulpit_config.json  # Theological categories, drift axes, avatars
├── db/                          # SQLite database files (gitignored)
└── tmp_audio/                   # Temporary audio storage (gitignored)
```

## Running
```bash
python main.py
```
Dashboard serves on `0.0.0.0:5000`.

## Required Environment Variables
- `YOUTUBE_API_KEY` — YouTube Data API v3 key
- `OPENAI_API_KEY` — OpenAI API key for transcription
- `DASHBOARD_PASSWORD` — Password for dashboard access (optional, open if not set)

## Optional Environment Variables
- `DATABASE_PATH` — SQLite path (default: `db/digital_pulpit.db`)
- `MAX_MINUTES_PER_RUN` — Cost safety rail (default: 180)
- `MAX_VIDEOS_PER_RUN` — Cost safety rail (default: 120)
- `KEEP_AUDIO_ON_FAIL` — Keep audio files on failure for debugging (default: false)

## Pipeline Phases
1. **Vacuum**: Resolves channels → discovers videos (7-day window, excludes Shorts) → downloads audio → transcribes via OpenAI API
2. **Brain**: Keyword-based theological analysis → density scoring → drift axis computation → weekly Z-score reports
3. **Assembly**: Assigns quotes to 6 avatars via affinity scoring → generates cinematic weekly scripts

## Avatars
- Sully (Reformed), Elena (Charismatic), Tio (Global), Noni (Bridge), Dr. Thorne (Institution), Elias (The Signal)

## Recent Changes
- 2026-02-12: Full engine build — Vacuum, Brain, Assembly, Streamlit dashboard
