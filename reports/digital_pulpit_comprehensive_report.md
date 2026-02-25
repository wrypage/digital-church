# Digital Pulpit - Comprehensive Database Report

**Generated:** $(date '+%Y-%m-%d %H:%M:%S')

---

## Executive Summary

The Digital Pulpit project has successfully pivoted from YouTube transcription (blocked by API limits) to RSS feed transcription. The database now contains **230 high-quality sermon transcripts** from **47 churches** across multiple platforms.

### Key Metrics

| Metric | Count |
|--------|-------|
| **Active Channels** | 47 |
| **Total Episodes** | 235 |
| **Successful Transcripts** | 230 (97.9% success) |
| **Failed Episodes** | 5 (2.1%) |
| **Total Words Transcribed** | 1,483,240 |
| **Average Words/Sermon** | 6,449 |
| **Estimated Audio Hours** | ~154 hours |

---

## Channel Breakdown by Source

### RSS Feeds (Subsplash)
- **Channels:** 40
- **Episodes:** 200
- **Transcripts:** 200 (100% success)
- **Average Words:** 6,456
- **Source:** Direct RSS feeds from Subsplash platform

### Apple Podcasts
- **Channels:** 7
- **Episodes:** 35
- **Transcripts:** 30 (85.7% success)
- **Average Words:** 6,804
- **Failed:** The Potter's House (5 episodes - audio download issues)
- **Source:** iTunes Lookup API → RSS extraction

---

## Top 15 Channels by Content Volume

| Rank | Channel | Source | Transcripts | Total Words | Avg Words |
|------|---------|--------|-------------|-------------|-----------|
| 1 | Outreach Center Church | RSS | 5 | 55,600 | 11,120 |
| 2 | Calvary Chapel Fort Lauderdale | RSS | 5 | 47,256 | 9,451 |
| 3 | Calvary Chapel Chattanooga | RSS | 5 | 47,107 | 9,421 |
| 4 | Calvary Chapel Old Bridge | RSS | 5 | 44,875 | 8,975 |
| 5 | The Village Church | Apple | 5 | 42,854 | 8,571 |
| 6 | The Expositor's Podcast | RSS | 5 | 40,237 | 8,047 |
| 7 | Elevation with Steven Furtick | Apple | 5 | 40,158 | 8,032 |
| 8 | Touching Lives with Dr. James Merritt | RSS | 5 | 40,107 | 8,021 |
| 9 | Emmanuel Church | RSS | 5 | 40,023 | 8,005 |
| 10 | Nexus Church | RSS | 5 | 39,493 | 7,899 |
| 11 | The River Church Podcast | RSS | 5 | 38,041 | 7,608 |
| 12 | Word of Grace Bible Church | RSS | 5 | 37,845 | 7,569 |
| 13 | Becoming Something (Jonathan Pokluda) | RSS | 5 | 36,799 | 7,360 |
| 14 | Lifepoint Church (Pastor Jeff Kapusta) | RSS | 5 | 35,927 | 7,185 |
| 15 | Prophecy Watchers | RSS | 5 | 35,474 | 7,095 |

---

## Pipeline Run History

### Successful Runs

| Run | Type | Date | Episodes | Minutes | Status |
|-----|------|------|----------|---------|--------|
| 14 | RSS Tier 1 | 2026-02-18 | 72 | 3,000 | ✅ Completed |
| 15 | RSS Tier 1 & 2 | 2026-02-18 | 125 | 4,865 | ✅ Completed |
| 16 | Apple Podcasts | 2026-02-18 | 30 | 1,200 | ✅ Completed |

**Total Processing Time:** ~9 hours (includes transcription, not download)
**Success Rate:** 230/235 = 97.9%

### Failed Attempts (Pre-RSS Pivot)

Runs 1-13 were vacuum/YouTube API attempts that failed due to API blocks (403/429 errors).

---

## Transcript Quality Metrics

### Technical Quality
- **Transcription Engine:** Whisper (OpenAI) - Base Model
- **Language:** 100% English
- **Provider:** whisper_local
- **Word Count Range:** 1,193 - 12,040 words
- **Median Length:** 6,643 words
- **Speaking Rate:** ~157.5 words/minute (fast preaching style)

### Quality Assessment
- ✅ **Zero empty transcripts**
- ✅ **Zero NULL word counts**
- ✅ **Proper sentence endings** (no mid-sentence truncations)
- ✅ **Consistent metadata** (language, provider, model)
- ⚠️ **Minor issue:** Elevation podcast contains commercial ads (~3-5% of content)

---

## Feed Distribution

### Subsplash RSS Feeds (40 channels)
From two validation batches:
- **Batch 1:** 15 feeds (Tier 1 emphasis on exposition)
- **Batch 2:** 25 feeds (All Tier 1 & Tier 2)

**Quality Criteria:**
- ✅ Active (updated within 7 days)
- ✅ Long-form (30-70 minutes)
- ✅ Theological depth
- ✅ Clean metadata
- ✅ Consistent publishing

### Apple Podcast Feeds (7 channels)
Large megachurch podcasts:
1. Elevation Church (Steven Furtick)
2. The Village Church (Matt Chandler)
3. Life.Church (Craig Groeschel)
4. North Point Community (Andy Stanley)
5. The Potter's House (T.D. Jakes) - *Failed*
6. Life Church Assembly of God
7. New Beginnings Assembly of God

---

## Comparison to Original Plan

### YouTube Plan (Archived)
- **Channels Attempted:** 137
- **Status:** All blocked by YouTube API (403/429 errors)
- **Data Preserved:** channels_legacy & videos_legacy tables
- **Reference File:** data/channels_with_ids.csv

### Current RSS/Podcast Strategy
- **Channels Active:** 47
- **Success Rate:** 97.9%
- **Total Transcripts:** 230 (vs 0 from YouTube)
- **Advantage:** No API limits, reliable access

---

## Database Statistics

### Active Tables
- **channels:** 47 rows
- **videos:** 235 rows
- **transcripts:** 230 rows
- **runs:** 16 rows

### Legacy Tables (Archived)
- **channels_legacy:** 137 YouTube channels
- **videos_legacy:** 153 YouTube video attempts

### Video Status Breakdown
- **Transcribed:** 230 (97.9%)
- **Error:** 5 (2.1% - all from The Potter's House)

---

## Notable Findings

### Content Diversity
- **Denominational Range:** Calvary Chapel, Assembly of God, Non-denominational, Baptist, etc.
- **Teaching Styles:** Expositional, topical, series-based
- **Church Sizes:** Megachurches (Elevation, Life.Church) to smaller congregations

### Theological Emphasis
Most feeds prioritize:
- Verse-by-verse exposition
- Book studies (Romans, 1 Corinthians, etc.)
- Topical series (Breaking Free From Broke, Moving Forward, etc.)

### Technical Observations
- **Longest Sermon:** 12,040 words (Calvary Chapel Fort Lauderdale)
- **Shortest Sermon:** 1,193 words (Connect with Skip Heitzig - devotional format)
- **Most Verbose Church:** Outreach Center (11,120 avg words)
- **Fastest Speaker:** 190 WPM (Life Church Assembly of God)

---

## Next Steps

### Recommendations
1. **Expand Corpus:** Add more Apple Podcast feeds
2. **Fix Potter's House:** Investigate authentication requirements for Acast RSS
3. **Filter Elevation Ads:** Post-process to remove commercial content
4. **Quality Audit:** Run periodic transcript quality checks
5. **Brain Analysis:** Ready for semantic analysis and drift detection

### Export Files
- ✅ **channels_complete_report.csv** - Full channel listing with metrics
- ✅ **feeds.txt** - All 43 RSS feed URLs
- ✅ **transcript_quality_findings.md** - Detailed quality audit

---

*Report generated from db/digital_pulpit.db*
*For questions or issues: See project documentation*
