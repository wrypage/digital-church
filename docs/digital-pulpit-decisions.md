# Digital Pulpit — Decisions Log
Version 1.0 — April 2026
Last updated: 2026-04-22

Read this before touching: analysis_engine.py, digital_pulpit_config.json,
channels.csv, schema.sql, vacuum pipeline, climate engine, or avatar system.

It exists so future sessions do not repeat approaches already evaluated and abandoned.

Entry format:
### [Component] — [ACCEPTED / REJECTED / SUPERSEDED] — [Date]
**Decision:** One sentence.
**What was tried:** What approach was evaluated.
**Why:** The actual reason — specific.
**What replaced it:** If rejected.
**Cost impact:** ↑ higher / ↓ lower / — neutral / unknown
**Current status:** Confirmed working approach.

---

## 1. Ingest pipeline

### YouTube audio scraping — REJECTED — 2026-02

**Decision:** Replace YouTube audio download (yt-dlp + Whisper) with RSS
transcript feeds as the primary ingest method.

**What was tried:** yt-dlp to download audio, local Whisper to transcribe.

**Why rejected:** YouTube constantly changes its API and bot detection.
yt-dlp breaks frequently. Legal/ethical concerns about scraping. High local
compute cost for Whisper transcription. RSS feeds are more reliable, legally
sound, and don't require audio processing.

**What replaced it:** RSS feed parsing with pre-existing transcript text.
build_rss_transcript_database.py handles this.

**Cost impact:** ↓ Significant reduction — no audio storage, no Whisper compute.

**Current status:** RSS feeds confirmed working. 230 transcripts, 1.48M words
ingested from 47 channels at 97.9% success rate.

---

### YouTube channel scope — 8 → 139 channels — ACCEPTED — 2026-02

**Decision:** Scale from 8 major national channels to 139 channels including
Rochester local, Reformed/Traditional, Historic African American, Calvary Chapel,
and Charismatic/Revivalist orientations.

**Why:** National analysis requires representational breadth across orientations.
8 channels was too narrow and skewed toward Modern Evangelical megachurches.

**Cost impact:** ↑ More channels = more API quota usage and transcription volume.
De-duplication prevents double-processing.

**Current status:** channels.csv has 139 channels with Primary Orientation column.
Rochester channels included: Koinonia Fellowship (Ray Viola), The Father's House,
Browncroft Community.

---

### SQLite database locking — WAL mode — ACCEPTED — 2026-02-13

**Decision:** Configure SQLite in WAL (Write-Ahead Logging) mode with immediate
transactions to prevent locking conflicts between Streamlit dashboard and pipeline.

**What was tried:** Default SQLite journal mode caused locking when Streamlit
read the database while vacuum was writing.

**Why accepted:** WAL mode allows concurrent reads and writes. Immediate
transactions prevent deadlocks.

**Cost impact:** — neutral.

**Current status:** Should be in db.py. Verify on reactivation.

---

### Schema function naming — standardized — ACCEPTED — 2026-02-13

**Decision:** Standardize all database upsert functions to `insert_or_ignore_video`
naming convention. Remove `upsert_video` variant.

**What was tried:** Mixed naming (upsert_video vs insert_or_ignore_video) caused
silent failures when one function called the wrong variant.

**Cost impact:** — neutral.

**Current status:** Resolved in February sprint.

---

### Logical sermon collapsing — DESIGNED, NOT BUILT — 2026-02-21

**Decision:** Add a collapsing layer to detect and merge multi-part sermon
broadcasts (Part A/B, Episode 1/2) into single logical sermon records.

**Why needed:** Multi-part sermons were counted as separate sermons, artificially
inflating convergence metrics and claim counts.

**Current status:** UNRESOLVED — designed but not implemented. Must be built
before metrics can be trusted at scale.

---

## 2. Brain / analysis engine

### Topical clustering → emphasis detection — SUPERSEDED — 2026-02-22

**Decision:** Replace semantic similarity clustering with emphasis imbalance
detection in the climate engine.

**What was tried:** Clustering sermons by topic similarity to find convergence.

**Why superseded:** Topical clustering answers "what exists in these sermons"
not "what dominates the preaching atmosphere." These are completely different
questions. A sermon corpus will always contain Grace, Sin, Hope — the question
is which is running disproportionately high or low relative to baseline.

**What replaced it:** Z-score drift detection on four axes: Grace/Effort,
Hope/Fear, Doctrine/Experience, Scripture/Story. Surface gravity accumulation,
asymmetry ratios, acceleration vectors, tone polarity shifts.

**Cost impact:** — neutral (same data, different calculation).

**Current status:** PARTIALLY RESOLVED. Z-score drift designed and coded.
Climate engine emphasis detection fix was the confirmed next move at last
assessment — not verified complete. Must verify on reactivation.

---

### Summary-first analysis — ACCEPTED — 2026-02-22

**Decision:** Route Brain analysis through cleaned sermon summaries rather than
full transcripts. Use full transcripts only for quote extraction and evidence.

**Why:** Full transcript analysis produces noise and hallucinated themes from
filler content, repeated phrases, and non-theological language. Summaries
concentrate theological signal.

**Cost impact:** ↓ Significant — fewer tokens per sermon in analysis pass.

**Current status:** Confirmed as the correct architecture. Was reinstated as
the controlled next move at last assessment — verify it's working on reactivation.

---

### Config versioning — ACCEPTED — ongoing

**Decision:** Every change to digital_pulpit_config.json increments version
and adds a patch_notes entry describing what changed.

**Current version:** 5.1.1

**Patch notes from v5.1.1:**
- Unified detector output naming to *_Match
- Added high-ROI tag overrides (Justification, Atonement, Trinity)
- Expanded affect vocabulary (Peace, Shame)
- Clarified high_yield_metrics sources (tags vs detectors)
- Added authority disambiguation guidance

**Rule:** Never change the config without updating version and patch_notes.

---

## 3. Avatar system

### Free-form debate → moderator model — ACCEPTED — 2026-02-14

**Decision:** Elias moderates and routes queries to specialist avatars rather
than all avatars debating freely.

**What was tried:** Multi-agent debate format where all 6 avatars respond
to the same prompt.

**Why rejected:** Free-form debate produces cartoon personalities, register
drift, and repetitive structure. The intellectual differences between Sully
(Reformed), Elena (Charismatic), Tio (Global), Noni (Bridge), Dr. Thorne
(Institution), and Elias (Signal) are lost when they all argue the same point.

**What replaced it:** Elias as moderator. Drift template routing determines
which avatar leads (AFFECT_LEADS → Elena, DOCTRINE_LEADS → Sully, etc.).

**Cost impact:** ↓ Prompt caching on moderator reduces per-run cost.

**Current status:** Designed in script_generator.py. Character affinities
in digital_pulpit_config.json.

---

### Lodestar framing — drift policing → climate listening — ACCEPTED — 2026-02-22

**Decision:** Reframe the entire system from "theological drift detection"
(surveillance/judgment) to "climate listening" (pastoral/observational).

**Why:** The language of drift detection implies the system is policing
orthodoxy. The language of climate listening implies the system is paying
pastoral attention. This shapes every output — from how Elias speaks to
what the weekly report surfaces.

**Elias as the signal:** "The quiet man in the back pew with his Bible open."
Not a judge. Not an analyst. A listener.

**Cost impact:** — neutral (reframing, not rebuild).

**Current status:** Accepted. Must be enforced in every prompt and output.

---

## 4. Infrastructure

### Replit → local + GitHub — SUPERSEDED — 2026-04

**Decision:** Move development from Replit to local machine with GitHub as
version control.

**What happened:** February sprint was built primarily in Replit. Advanced
files (139-channel channels.csv, analysis_engine.py, digital_pulpit_config.json
v5.1.1, avatar system, script_generator.py) were never committed to GitHub.

**Why it matters:** Advanced system existed only in Replit project and local
iCloud folder. Risk of loss if Replit project expired.

**Current status:** All advanced files committed to GitHub 2026-04-22.
digital-church repo is now the authoritative source.

---

### pulpit.db committed to git — PROBLEM — 2026-04-22

**Decision:** Remove pulpit.db from git tracking. Binary database files
should not be in version control.

**Why:** Database grows with every Vacuum run. Committing it bloats the repo
and creates merge conflicts. It also may contain scraped sermon data with
unclear licensing.

**Current status:** UNRESOLVED. pulpit.db is currently in the repo.
Add to .gitignore and remove from tracking on next session.

---

## 5. Active unresolved issues

### Node.js 24 deprecation warning — 2026-04-22
GitHub Actions vacuum workflow uses Node.js 20. Will stop working June 2, 2026.
Fix: add FORCE_JAVASCRIPT_ACTIONS_TO_NODE24: true to workflow env section.
Low effort, must be done before June 2.

### Climate engine emphasis detection — 2026-04-22
Was identified as broken (topical clustering not emphasis detection) and a fix
was designed (z-score drift). Not verified as complete on reactivation.
Must test before trusting any climate reports.

### DATABASE_PATH config issue — 2026-04-22
System health check in February found config.py DATABASE_PATH may point to
non-existent directory, making pulpit.db inaccessible to the application.
Verify path on reactivation before running Vacuum.

### Logical sermon collapsing not built — 2026-04-22
Multi-part sermon merging designed but not implemented. Metrics are inflated
until this is built.

### pulpit.db in git — 2026-04-22
Binary database file should be gitignored. Remove from tracking.

### Rochester vs national reactivation — 2026-04-22
Strategic decision pending. Rochester-first (3 local channels) validates the
model at low cost. National (139 channels) is the full vision but higher cost
and complexity. Local Christian app idea is the commercial framing for
Rochester-first.

### Pastor Portal not built — 2026-04-22
Designed as voluntary opt-in with immediate transcript reward. Not implemented.
Required for ethical, sustainable ingestion at scale.
