#!/usr/bin/env python3
"""Test Brain v2 UPSERT behavior."""

import sys
import sqlite3
sys.path.insert(0, '.')

from engine import brain

DATABASE_PATH = "db/digital_pulpit.db"

# Get the first analyzed video
conn = sqlite3.connect(DATABASE_PATH)
conn.row_factory = sqlite3.Row

row = conn.execute("""
    SELECT br.video_id, v.channel_id, v.title, v.published_at, t.full_text, t.word_count
    FROM brain_results br
    JOIN videos v ON br.video_id = v.video_id
    JOIN transcripts t ON v.video_id = t.video_id
    LIMIT 1
""").fetchone()

if not row:
    print("No analyzed videos found!")
    sys.exit(1)

video_id = row['video_id']
channel_id = row['channel_id']

print("="*80)
print(f"Testing UPSERT on video: {video_id}")
print("="*80)

# Count before rerun
before_results = conn.execute("SELECT COUNT(*) FROM brain_results WHERE video_id = ?", (video_id,)).fetchone()[0]
before_evidence = conn.execute("SELECT COUNT(*) FROM brain_evidence WHERE video_id = ?", (video_id,)).fetchone()[0]

print(f"Before rerun: {before_results} result(s), {before_evidence} evidence snippets")

# Reanalyze the same video
cfg = brain.load_brain_config()
brain.ensure_tables(conn)

brain.analyze_one(
    conn=conn,
    cfg=cfg,
    video_id=row['video_id'],
    channel_id=row['channel_id'],
    title=row['title'] or "",
    published_at=row['published_at'] or "",
    full_text=row['full_text'] or "",
    word_count=int(row['word_count'] or 0),
)

# Count after rerun
after_results = conn.execute("SELECT COUNT(*) FROM brain_results WHERE video_id = ?", (video_id,)).fetchone()[0]
after_evidence = conn.execute("SELECT COUNT(*) FROM brain_evidence WHERE video_id = ?", (video_id,)).fetchone()[0]

print(f"After rerun:  {after_results} result(s), {after_evidence} evidence snippets")

# Verify
assert after_results == 1, f"UPSERT failed: expected 1 result, got {after_results}"
assert after_evidence == before_evidence, f"Evidence cleanup failed: expected {before_evidence}, got {after_evidence}"

print("\n✅ UPSERT TEST PASSED: No duplicates, evidence properly cleaned")
print("="*80)

conn.close()
