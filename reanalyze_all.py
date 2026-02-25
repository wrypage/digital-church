#!/usr/bin/env python3
"""Re-analyze all transcripts to update with new z-score calculation."""

import sys
import sqlite3
sys.path.insert(0, '.')

from engine import brain

DATABASE_PATH = "db/digital_pulpit.db"

print("="*80)
print("Re-analyzing ALL transcripts with updated z-score calculation")
print("="*80)

conn = sqlite3.connect(DATABASE_PATH)
conn.row_factory = sqlite3.Row

# Fetch all transcripts (not just unanalyzed)
q = """
SELECT
    t.video_id,
    t.full_text,
    t.word_count,
    v.channel_id,
    v.title,
    v.published_at
FROM transcripts t
JOIN videos v ON t.video_id = v.video_id
WHERE t.full_text IS NOT NULL
  AND t.word_count IS NOT NULL
ORDER BY v.published_at DESC
"""

rows = conn.execute(q).fetchall()
print(f"Found {len(rows)} transcripts to re-analyze\n")

cfg = brain.load_brain_config()
brain.ensure_tables(conn)

processed = 0
for i, r in enumerate(rows, 1):
    if i % 50 == 0:
        print(f"Progress: {i}/{len(rows)}")

    brain.analyze_one(
        conn=conn,
        cfg=cfg,
        video_id=r['video_id'],
        channel_id=r['channel_id'],
        title=r['title'] or "",
        published_at=r['published_at'] or "",
        full_text=r['full_text'] or "",
        word_count=int(r['word_count'] or 0),
    )
    processed += 1

conn.close()

print(f"\n{'='*80}")
print(f"Re-analysis complete. Updated {processed} transcripts.")
print("="*80)
