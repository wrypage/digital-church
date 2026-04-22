# Digital Pulpit — Claude Code Context

## What this project is
A sermon analysis system that converts global sermon data into theological intelligence.
Three-stage pipeline: Vacuum (ingest) → Brain (analysis) → Assembly (scripting).
139 channels tracked. 6 avatars: Sully, Elena, Tio, Noni, Dr. Thorne, Elias.

## Orientation URLs — paste into Claude.ai at session start
https://raw.githubusercontent.com/wrypage/digital-church/main/digital-pulpit-state-of-the-project.md
https://raw.githubusercontent.com/wrypage/digital-church/main/digital-pulpit-decisions.md

## Key files
- channels.csv — 139 channels with orientation and location
- digital_pulpit_config.json — v5.1.1 theological scoring config
- analysis_engine.py — DigitalPulpitBrain class
- script_generator.py — AssemblyScriptDirector class
- main.py — Streamlit War Room dashboard
- schema.sql — SQLite schema (7 tables)
- Master Avatar Guide.docx — character profiles for all 6 avatars
- Master Instructions.docx — full system specification

## Rules
- Read digital-pulpit-decisions.md before touching any pipeline code
- Never modify channels.csv without checking Primary Orientation column logic
- Never change digital_pulpit_config.json version without updating patch_notes
- Dashboard password is in Streamlit secrets, not in code
