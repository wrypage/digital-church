# Digital Pulpit Corpus State

**Current Version:** v2.1-5.3 (Summary Generator v2.1 + Config v5.3)
**Date:** 2026-02-24
**Status:** ✅ Stable

---

## Quick Reference

| Component | Version | Status | Last Updated |
|-----------|---------|--------|--------------|
| Summary Generator | **v2.1** | ✅ Production | 2026-02-24 |
| Config (keywords) | **v5.3** | ✅ Production | 2026-02-24 |
| Summaries | **232/235 regenerated** | ✅ Current | 2026-02-24 |
| Brain Results | **235/235 computed** | ✅ Current | 2026-02-24 |

---

## Corpus Metrics

### Theological Density
- **Corpus average:** 89.0 keywords per 1000 words
- **Previous:** 60.7 (+47% improvement)

### Axis Averages
- **hope_vs_fear:** 0.635 (was 0.397)
- **grace_vs_effort:** -0.061 (was -0.139)
- **doctrine_vs_experience:** 0.052 (was -0.226)
- **scripture_vs_story:** 0.205 (was -0.057)

---

## Version History

### v2.1-5.3 (2026-02-24) — CURRENT ✅
- **Summary Generator v2.1:** Closing movement preservation
- **Config v5.3:** Expanded hope/grace keywords
- **Impact:** 372 axis flips, +60% hope capture
- **Snapshot:** `db/snapshots/digital_pulpit_v2.1_config5.3_2026-02-24.db`
- **Documentation:** [db/snapshots/VERSION_2.1_CONFIG_5.3.md](db/snapshots/VERSION_2.1_CONFIG_5.3.md)

### v2.0-5.2 (Baseline)
- **Original state** before regeneration
- **Baseline data:** `out/brain_results_before_v2_1.csv`

Full history: [db/snapshots/CHANGELOG.md](db/snapshots/CHANGELOG.md)

---

## File Locations

### Database
- **Current:** `db/digital_pulpit.db` (31 MB)
- **Snapshots:** `db/snapshots/` (versioned backups)

### Configuration
- **Theological config:** `data/digital_pulpit_config.json` (v5.3)
- **Summary prompt:** `engine/regenerate_summaries_v2.py` (v2.1)

### Reports
- **Corpus report:** `out/corpus_regeneration_report.txt`
- **Audit samples:** `out/audit/` (15 files + index)
- **Baseline:** `out/brain_results_before_v2_1.csv`

### Scripts
- **Regenerate summaries:** `engine/regenerate_summaries_v2.py`
- **Recompute brain:** `engine/brain.py`
- **Generate report:** `generate_corpus_report.py`
- **Run audit:** `audit_flips.py`
- **Full workflow:** `run_full_corpus_regeneration.py`

---

## Common Operations

### Restore current version:
```bash
cp db/snapshots/digital_pulpit_v2.1_config5.3_2026-02-24.db db/digital_pulpit.db
```

### Regenerate specific sermons:
```bash
python -m engine.regenerate_summaries_v2 --db db/digital_pulpit.db \
  --video_ids VIDEO_ID_1 VIDEO_ID_2
```

### Regenerate entire corpus:
```bash
python -m engine.regenerate_summaries_v2 --db db/digital_pulpit.db --all
```

### Recompute Brain for all sermons:
```bash
python -m engine.brain --recompute
```

### Generate comparison report:
```bash
python generate_corpus_report.py
```

### Run random audit:
```bash
python audit_flips.py
```

---

## Validation Status

### Test Case: 1 Timothy 1:1-3
- ✅ hope_vs_fear: 0.750 (was 0.000)
- ✅ Closing movement captured
- ✅ Rhetorical emphasis preserved

### Corpus-Wide
- ✅ 372 axis flips validated
- ✅ 87 hope reversals documented
- ✅ 15 random audits extracted

---

## Known Issues

### Minor
- 3 sermons failed regeneration (API quota exceeded)
  - fdc7b1a4f1b12464
  - fdff17fa95877d31
  - fff27fa6b5ef676a
- These retain v2.0 summaries (acceptable)

### Future Improvements
- Consider regenerating failed 3 when API quota available
- Monitor for any false positives in axis flips

---

## Maintenance

**Last regeneration:** 2026-02-24
**Next review:** As needed
**Snapshot frequency:** Before major changes

**Maintained by:** Stephen Morse + Claude Sonnet 4.5

---

**Quick Status Check:**
```bash
# Check corpus version
head -1 db/snapshots/CHANGELOG.md

# Count sermons
sqlite3 db/digital_pulpit.db "SELECT COUNT(*) FROM brain_results"

# View recent averages
python generate_corpus_report.py | grep "CORPUS-WIDE"
```
