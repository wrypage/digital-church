# Digital Pulpit Corpus Version History

This document tracks major corpus states and regenerations.

---

## v2.1-5.3 (2026-02-24) — CURRENT

**Snapshot:** `digital_pulpit_v2.1_config5.3_2026-02-24.db`

### Summary
Major corpus regeneration with enhanced hope/grace capture.

### Changes
- ✅ Summary Generator v2.1 deployed (closing movement preservation)
- ✅ Config v5.3 deployed (expanded hope/grace keywords)
- ✅ 232/235 summaries regenerated
- ✅ All 235 Brain results recomputed

### Impact
- hope_vs_fear: +0.238 corpus-wide improvement
- grace_vs_effort: +0.079 improvement
- theological_density: +28.4 increase
- **372 axis flips** across corpus

### Files
- Documentation: [VERSION_2.1_CONFIG_5.3.md](VERSION_2.1_CONFIG_5.3.md)
- Full report: [../../out/corpus_regeneration_report.txt](../../out/corpus_regeneration_report.txt)
- Audit samples: [../../out/audit/](../../out/audit/)

---

## v2.0-5.2 (Baseline, pre-2026-02-24)

**Snapshot:** `brain_results_before_v2_1.csv` (Brain results only)

### Summary
Original corpus state before v2.1 regeneration. Summaries generated with original Summary Generator v2.0 prompt.

### Known Issues
- Hope language systematically under-captured
- Grace language compressed out in closing movements
- Theological density lower than optimal

### Files
- Baseline: [../../out/brain_results_before_v2_1.csv](../../out/brain_results_before_v2_1.csv)

---

## Version Numbering

Format: `vX.Y-C.Z`
- `X.Y` = Summary Generator version
- `C.Z` = Config version

Example: `v2.1-5.3` = Summary Generator v2.1 + Config v5.3

---

## Restoring Versions

### Restore current version (v2.1-5.3):
```bash
cp db/snapshots/digital_pulpit_v2.1_config5.3_2026-02-24.db db/digital_pulpit.db
```

### Compare any two states:
Modify `generate_corpus_report.py` to point to different baseline files.

---

## Snapshot Storage

All snapshots stored in `db/snapshots/` with naming convention:
```
digital_pulpit_v{SUMMARY_VERSION}_config{CONFIG_VERSION}_{DATE}.db
```

Full database snapshots are ~31MB each.

---

**Maintained by:** Claude Sonnet 4.5
**Last updated:** 2026-02-24
