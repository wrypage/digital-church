# Brain v2 Hardening + Assembly Agenda - Implementation Report

**Date:** 2026-02-18
**Status:** ✅ COMPLETE

---

## Changes Implemented

### 1. Database Migrations ✅

Executed SQL migrations on `db/digital_pulpit.db`:

```sql
-- Prevent duplicate brain_results rows per video
CREATE UNIQUE INDEX idx_brain_results_video_unique ON brain_results(video_id);

-- Help agenda queries
CREATE INDEX idx_videos_published_at ON videos(published_at);
CREATE INDEX idx_videos_channel_id ON videos(channel_id);

-- Evidence indexes (created by brain.py ensure_tables)
CREATE INDEX idx_brain_evidence_axis ON brain_evidence(axis);
CREATE INDEX idx_brain_evidence_category ON brain_evidence(category);
```

### 2. engine/brain.py Modifications ✅

**A) Prevent duplicate evidence on reruns (line 549)**
```python
# A) Prevent duplicate evidence on reruns
conn.execute("DELETE FROM brain_evidence WHERE video_id = ?", (video_id,))
```

**B) Make brain_results insert idempotent (line 625)**
```python
# B) Make brain_results insert idempotent (UPSERT)
INSERT INTO brain_results (...)
VALUES (...)
ON CONFLICT(video_id) DO UPDATE SET
  theological_density=excluded.theological_density,
  ...
  analyzed_at=CURRENT_TIMESTAMP
```

**C) Baseline correctness when history is missing (line 562)**
```python
# C) Baseline correctness: check if we have valid baseline data
baseline_ok = bool(axis_mean) and any((float(s) > 1e-9) for s in axis_std.values())

if not baseline_ok:
    # Insufficient history: set empty zscores and special drift level
    axis_z = {}
    drift_level = "insufficient_history"
else:
    # Compute zscores normally
    axis_z = {...}
    drift_level = classify_drift(axis_z)
```

**D) Added evidence indexes to ensure_tables (line 449)**
```python
conn.execute("CREATE INDEX IF NOT EXISTS idx_brain_evidence_axis ON brain_evidence(axis);")
conn.execute("CREATE INDEX IF NOT EXISTS idx_brain_evidence_category ON brain_evidence(category);")
```

### 3. engine/agenda.py Created ✅

New file that:
- Selects analyzed sermons from last N days (default 7)
- Computes drift magnitude from `raw_scores_json['zscores']['axes']`
- Picks 3 buckets:
  1. **drift**: anomaly/strong/moderate drift (top 5 by drift_mag)
  2. **imbalance**: extreme axis scores (top 5 by max abs axis score)
  3. **stable**: exemplars with low drift (few items)
- Attaches evidence snippets from `brain_evidence` for each item
- Outputs JSON when run as `__main__`

**Usage:**
```bash
python -m engine.agenda          # Last 7 days
python -m engine.agenda 14       # Last 14 days
```

---

## Acceptance Tests

### Test 1: brain_results has rows after smoke test ✅
```
brain_results: 4 rows (2 from first run, 2 from second run)
```

### Test 2: brain_evidence has rows for analyzed videos ✅
```
brain_evidence: 83 rows total
- video 0847a2a9176c3b12: 19 snippets
- video 91e275a644aefd71: 22 snippets
- video 70a7541787a951a8: 19 snippets
- video 79eaac371efcb49b: 23 snippets
```

### Test 3: UPSERT works (no duplicates on rerun) ✅
```
Reanalyzed same video: 1 result before, 1 result after
UPSERT prevented duplicate rows
```

### Test 4: Evidence delete-then-insert works ✅
```
Reanalyzed same video: 19 evidence snippets before, 19 after
Evidence properly cleaned and recreated
```

### Test 5: agenda.py outputs valid JSON ✅
```json
{
  "drift": [],
  "imbalance": [...],
  "stable": [...],
  "metadata": {
    "days": 7,
    "total_sermons": 4,
    "drift_count": 0,
    "stable_count": 4,
    "generated_at": "2026-02-19T02:52:39.279455Z"
  }
}
```

Each item has `evidence[]` array with snippets.

---

## Smoke Test Results

### First Run
```bash
python3 test_brain_v2.py
# Processed 2 transcripts
# Created brain_results and brain_evidence entries
```

### UPSERT Test
```bash
python3 test_brain_upsert.py
# ✅ UPSERT TEST PASSED: No duplicates, evidence properly cleaned
```

### Agenda Test
```bash
python3 -m engine.agenda
# ✅ Valid JSON output with drift, imbalance, stable buckets
```

---

## Definition of Done ✅

- [x] brain.py hardened (UPSERT + evidence cleanup + baseline correctness)
- [x] agenda.py added and tested
- [x] Smoke test passes (2 transcripts)
- [x] All 5 acceptance tests pass
- [x] Ready to run full analysis on all 235 transcripts

---

## Next Steps

### Run Full Analysis
```bash
python3 -m engine.brain  # Analyze all 235 transcripts
```

### Generate Full Agenda
```bash
python3 -m engine.agenda 14 > reports/agenda_$(date +%Y%m%d).json
```

### Monitor Progress
```bash
sqlite3 db/digital_pulpit.db "SELECT COUNT(*) FROM brain_results;"
sqlite3 db/digital_pulpit.db "SELECT COUNT(*) FROM brain_evidence;"
```

---

## Files Modified/Created

### Modified
- `engine/brain.py` - Added UPSERT, evidence cleanup, baseline correctness
- `engine/config.py` - (import path updated)

### Created
- `engine/agenda.py` - Assembly agenda generator
- `test_brain_v2.py` - Smoke test script
- `test_brain_upsert.py` - UPSERT verification script
- `BRAIN_V2_IMPLEMENTATION.md` - This document

---

**Implementation Status:** ✅ COMPLETE
**All Requirements Met:** YES
**Ready for Production:** YES
