# Digital Pulpit – Master System Spec (working)

## Goal
Track sermons at scale: discover videos, download audio, transcribe, store in DB, then analyze and report.

## Non-negotiables
- Single source of truth is the database (no “files-only” workflow).
- Costs must be bounded (max videos + max minutes per run).
- Config-driven channel selection (channels.csv or DB channels table).
- Clear status pipeline (discovered → downloaded → transcribed → queued_for_brain → analyzed → scripted).
- Secrets via environment variables (OPENAI_API_KEY, YOUTUBE_API_KEY).

## Modules
- engine/youtube.py: discovery + metadata
- engine/vacuum.py: orchestration of discover/download/transcribe
- engine/transcription.py: transcription
- engine/db.py: DB access + status transitions
- engine/brain.py: analysis (to be defined by our spec)
- engine/assembly.py: script/outputs (to be defined by our spec)

## UI
- Optional dashboard (Streamlit ok) but must not dictate core logic.
