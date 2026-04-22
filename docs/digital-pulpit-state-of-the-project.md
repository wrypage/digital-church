# Digital Pulpit — State of the Project
Version 1.0 — April 2026
Last updated: 2026-04-22

For implementation details: digital-pulpit-architecture.md
For output feeling targets: digital-pulpit-voice-and-register.md
For judging sessions: digital-pulpit-review.md
For rejected approaches: digital-pulpit-decisions.md
For session ritual: digital-pulpit-session-playbook.md
For philosophical commitments: digital-pulpit-principles.md

---

## What this project is

A sermon analysis system that converts global evangelical preaching data into
theological intelligence. Not a sermon directory. Not a church review platform.
A listening system — the difference between surveillance and climate sensing.

The system processes YouTube and RSS sermon feeds through three stages:
Vacuum (ingest) → Brain (analysis) → Assembly (scripting). The output is not
a list of what was said. It is a map of what dominates — which theological
emphases are rising, which are receding, and what the cumulative climate of
evangelical preaching looks like across 139 channels in a given week.

The six avatars (Sully, Elena, Tio, Noni, Dr. Thorne, Elias) give that climate
a voice. Elias is the signal — the quiet meteorologist reading the atmosphere.
The others represent the traditions whose emphases the system tracks.

---

## The intended experience

A pastor opens the weekly report. Not a spreadsheet. A 60-90 second cinematic
script — data-grounded dialogue between the avatars — that surfaces the Drift
of the Week: which theological axis moved, in which direction, by how much.

The pastor should feel: *someone has been paying close attention to the whole
ecosystem, so I don't have to. And what they found is worth knowing.*

What a good run produces:
- Transcripts ingested across active channels, status tracked in pulpit.db
- Theological density scores (Dθ) calculated per sermon segment
- Z-score drift detected across Grace/Effort, Hope/Fear, Scripture/Story axes
- Weekly heartbeat report with drift metrics and top quotes
- Assembly script assigned to the right avatars by affinity
- HeyGen-ready 60-second dialogue arc

What bad output looks like:
- Topical clustering instead of emphasis detection ("what exists" not "what dominates")
- Generic AI voice in avatar scripts instead of character-specific register
- Inflated metrics from multi-part sermon fragments counted as separate sermons
- Climate reports that feel judgmental rather than observational

---

## Where the project is right now

**What is working — as of February 2026 sprint:**
- Vacuum pipeline ingesting from RSS feeds — proven more reliable than YouTube scraping
- 230 transcripts, 1.48M words ingested across 47 channels at 97.9% success rate
- DigitalPulpitBrain (analysis_engine.py) v5.1.1 — full theological scoring with
  gospel anchor proximity, targeted multipliers, verse/imperative detection
- Script generator (script_generator.py) — AssemblyScriptDirector with drift
  template routing (AFFECT_LEADS, DOCTRINE_LEADS, SCRIPTURE_LEADS, GLOBAL_LEADS)
- SQLite schema (schema.sql) — 7 tables: channels, videos, transcripts, quote_units,
  weekly_drift_reports, runs, run_videos
- Streamlit War Room dashboard (main.py) — password protected
- 6 avatar character system — Sully, Elena, Tio, Noni, Dr. Thorne, Elias
- channels.csv — 139 channels with Primary Orientation column
- All advanced files now committed to git — 2026-04-22

**What is not yet resolved:**
- System is dormant — Vacuum not running on a schedule since February 2026
- GitHub Actions vacuum workflow needs Node.js 24 update (daily warning emails)
- Climate engine was producing topical clustering rather than emphasis imbalance
  detection at last assessment — "what exists" not "what dominates"
- Summary-first analysis enforcement was the confirmed next move but not verified complete
- DATABASE_PATH config issue flagged — config.py may point to non-existent directory
- Pastor Portal designed but not built
- pulpit.db committed to git (should be gitignored — binary database file)
- Master Avatar Guide content not yet ingested into memory database

**Deliberately out of scope for now:**
- National scale reactivation — Rochester-first is the strategic consideration
- HeyGen video production integration
- Public-facing pastor portal
- Paid subscription model

---

## What has been learned

**RSS feeds beat YouTube scraping.** The pivot from YouTube audio download
(yt-dlp + Whisper) to RSS transcript feeds was the single most important
architectural decision. RSS is sustainable, legally sound, and more reliable.
YouTube scraping breaks constantly as YouTube changes its API.

**The climate engine must detect emphasis, not presence.** The critical failure
mode — discovered in the Tuning System to Lodestar session — is that semantic
clustering answers "what topics exist in these sermons" rather than "what is
dominating the preaching atmosphere." These are completely different questions.
The system must surface gravity accumulation, asymmetry ratios, and acceleration
vectors, not just topic frequency.

**The lodestar is listening, not policing.** The system evolved from "drift
detection" language (which implies surveillance and judgment) to "climate
sensing" language (which implies pastoral concern and attentiveness). This is
not just rhetorical — it shapes every design decision about what to surface
and how to frame it. Elias is the quiet man in the back pew with his Bible open.

**Summary-first analysis is the correct architecture.** Running Brain analysis
through cleaned sermon summaries rather than full transcripts reduces noise,
cost, and hallucinated themes. Full transcripts are used only for quote
extraction and evidence.

**Multi-part sermons inflate metrics.** Part A/B and Episode 1/2 broadcasts
from the same sermon counted as separate sermons, artificially inflating
convergence metrics. A logical sermon collapsing layer was designed to fix this.

**The avatar system needs a moderator, not a debate format.** Elias routes
queries to specialist agents (Sully, Elena, Tio, Noni, Dr. Thorne) rather
than letting them debate freely. Free-form multi-agent debate produces
cartoon personalities and register drift. The moderator model preserves
intellectual differences.

---

## Where the project is going

**Current phase:** Dormant — awaiting reactivation decision

**Strategic question:** Rochester-first or national scale?
Rochester is already in channels.csv (Koinonia Fellowship, The Father's House,
Browncroft Community). A Rochester-local version validates the model at low cost
with real pastoral relationships before scaling to 139 channels nationally.
The local Christian app idea is the commercial face of Digital Pulpit at local scale.

**Reactivation phase:**
1. Fix Node.js 24 warning in vacuum workflow
2. Verify DATABASE_PATH config is correct
3. Fix climate engine emphasis detection
4. Run Vacuum on Rochester channels only first
5. Generate first weekly report and evaluate

**After reactivation:**
- Pastor Portal build (voluntary opt-in, immediate transcript reward)
- Memory database bridge (Phase 10 of master memory plan)
- HeyGen script production
- Rochester → regional → national scale decision

---

## Next session goal

Fix Node.js 24 warning in GitHub Actions workflow. Verify DATABASE_PATH.
Decide: Rochester-first or national reactivation.

---

## The shortest possible summary

Digital Pulpit is a sermon analysis system — Vacuum → Brain → Assembly — that
detects theological climate across 139 evangelical channels using a v5.1.1
scoring engine and 6 avatar characters. 230 transcripts ingested, 1.48M words.
Built in a February 2026 sprint, now dormant. All advanced files committed to
git as of April 2026. Strategic question: reactivate nationally or pivot to
Rochester-local as the intelligence layer for a local Christian community app.
