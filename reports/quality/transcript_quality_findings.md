# Transcript Quality Audit Report

**Generated:** 2026-02-18 08:03:57

## Summary Metrics

- **Total Transcripts:** 108
- **Empty/NULL Text:** 0
- **NULL Word Count:** 0

### Word Count Statistics

- **Min:** 1,308
- **Median:** 6,643
- **Average:** 6,620.81
- **Max:** 12,040

### Distributions

**Language:**
- en: 108

**Provider:**
- whisper_local: 108

**Model:**
- base: 108

## Anomalies

✓ No duplicate video_ids found


## Suspect Transcripts (6)

| Transcript ID | Video ID | Word Count | Issues |
|---------------|----------|------------|--------|
| 41 | `4e492843c465beef` | 1308 | truncated |
| 99 | `5d1a7c6feaae7dbf` | 3645 | truncated |
| 88 | `68b4f06919a992b0` | 12040 | truncated |
| 13 | `502f7d9af91db6fa` | 10511 | truncated |
| 16 | `b33f91e7be7d7e59` | 9767 | truncated |
| 101 | `78db72267e74fa70` | 6643 | truncated |

## Recommended Actions


---

*For detailed transcript samples, see `transcript_quality_sample.csv`*
*For full metrics, see `transcript_quality_metrics.json`*
