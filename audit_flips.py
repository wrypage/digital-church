#!/usr/bin/env python3
"""
audit_flips.py

Select random audit cases from axis flips and export summaries/transcripts.
"""

import sqlite3
import random
import os
from typing import List, Dict, Tuple


def load_baseline(csv_path: str) -> Dict[str, Dict[str, float]]:
    """Load baseline brain_results from CSV."""
    baseline = {}
    with open(csv_path, 'r') as f:
        for line in f:
            parts = line.strip().split('|')
            if len(parts) == 6:
                video_id = parts[0]
                baseline[video_id] = {
                    'grace_vs_effort': float(parts[1]),
                    'hope_vs_fear': float(parts[2]),
                    'doctrine_vs_experience': float(parts[3]),
                    'scripture_vs_story': float(parts[4]),
                }
    return baseline


def get_current_results(db_path: str) -> Dict[str, Dict[str, float]]:
    """Get current brain_results from database."""
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    query = """
        SELECT video_id, grace_vs_effort, hope_vs_fear,
               doctrine_vs_experience, scripture_vs_story
        FROM brain_results
    """

    current = {}
    for row in con.execute(query):
        current[row['video_id']] = {
            'grace_vs_effort': row['grace_vs_effort'],
            'hope_vs_fear': row['hope_vs_fear'],
            'doctrine_vs_experience': row['doctrine_vs_experience'],
            'scripture_vs_story': row['scripture_vs_story'],
        }

    con.close()
    return current


def get_video_data(db_path: str, video_id: str) -> Dict:
    """Get title, summary, and transcript for a video."""
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    query = """
        SELECT v.title, v.channel_id, c.channel_name,
               t.summary_text, t.full_text
        FROM videos v
        JOIN transcripts t ON v.video_id = t.video_id
        LEFT JOIN channels c ON v.channel_id = c.channel_id
        WHERE v.video_id = ?
    """

    row = con.execute(query, (video_id,)).fetchone()
    con.close()

    if row:
        return {
            'title': row['title'],
            'channel_name': row['channel_name'] or 'Unknown',
            'summary': row['summary_text'] or '',
            'transcript': row['full_text'] or ''
        }
    return None


def sign(x: float) -> int:
    """Return sign of number."""
    if x > 0.05:
        return 1
    elif x < -0.05:
        return -1
    else:
        return 0


def find_flips(baseline: Dict, current: Dict) -> Tuple[List, List, List]:
    """Find flips for each axis."""
    hope_neg_to_pos = []
    grace_flips = []
    doctrine_flips = []

    for video_id in baseline.keys():
        if video_id not in current:
            continue

        b = baseline[video_id]
        c = current[video_id]

        # Hope: specifically -1 to +1
        if b['hope_vs_fear'] < -0.5 and c['hope_vs_fear'] > 0.5:
            hope_neg_to_pos.append((video_id, b['hope_vs_fear'], c['hope_vs_fear']))

        # Grace: any sign flip
        if sign(b['grace_vs_effort']) != sign(c['grace_vs_effort']):
            grace_flips.append((video_id, b['grace_vs_effort'], c['grace_vs_effort']))

        # Doctrine: any sign flip
        if sign(b['doctrine_vs_experience']) != sign(c['doctrine_vs_experience']):
            doctrine_flips.append((video_id, b['doctrine_vs_experience'], c['doctrine_vs_experience']))

    return hope_neg_to_pos, grace_flips, doctrine_flips


def write_audit_file(video_id: str, data: Dict, before: float, after: float,
                     axis: str, index: int, out_dir: str):
    """Write audit file for a video."""
    filename = f"{axis}_{index+1}_{video_id}.txt"
    filepath = os.path.join(out_dir, filename)

    with open(filepath, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write(f"AUDIT: {axis.upper()} FLIP\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Video ID: {video_id}\n")
        f.write(f"Title: {data['title']}\n")
        f.write(f"Channel: {data['channel_name']}\n")
        f.write(f"\n{axis} CHANGE: {before:.3f} → {after:.3f} (Δ{after - before:+.3f})\n")
        f.write("\n" + "=" * 80 + "\n")
        f.write("SUMMARY (v2.1)\n")
        f.write("=" * 80 + "\n\n")
        f.write(data['summary'])
        f.write("\n\n" + "=" * 80 + "\n")
        f.write("TRANSCRIPT\n")
        f.write("=" * 80 + "\n\n")
        f.write(data['transcript'])
        f.write("\n")

    print(f"✓ Written: {filename}")
    print(f"  {data['title']}")
    print(f"  {axis}: {before:.3f} → {after:.3f}\n")


def main():
    baseline = load_baseline('out/brain_results_before_v2_1.csv')
    current = get_current_results('db/digital_pulpit.db')

    hope_flips, grace_flips, doctrine_flips = find_flips(baseline, current)

    print(f"Found {len(hope_flips)} hope -1→+1 flips")
    print(f"Found {len(grace_flips)} grace sign flips")
    print(f"Found {len(doctrine_flips)} doctrine sign flips")
    print()

    # Select 5 random from each
    random.seed(42)  # For reproducibility

    hope_sample = random.sample(hope_flips, min(5, len(hope_flips)))
    grace_sample = random.sample(grace_flips, min(5, len(grace_flips)))
    doctrine_sample = random.sample(doctrine_flips, min(5, len(doctrine_flips)))

    out_dir = "out/audit"
    os.makedirs(out_dir, exist_ok=True)

    print("=" * 80)
    print("HOPE_VS_FEAR FLIPS (-1 → +1)")
    print("=" * 80)
    print()

    for i, (video_id, before, after) in enumerate(hope_sample):
        data = get_video_data('db/digital_pulpit.db', video_id)
        if data:
            write_audit_file(video_id, data, before, after, "hope_vs_fear", i, out_dir)

    print("=" * 80)
    print("GRACE_VS_EFFORT SIGN FLIPS")
    print("=" * 80)
    print()

    for i, (video_id, before, after) in enumerate(grace_sample):
        data = get_video_data('db/digital_pulpit.db', video_id)
        if data:
            write_audit_file(video_id, data, before, after, "grace_vs_effort", i, out_dir)

    print("=" * 80)
    print("DOCTRINE_VS_EXPERIENCE SIGN FLIPS")
    print("=" * 80)
    print()

    for i, (video_id, before, after) in enumerate(doctrine_sample):
        data = get_video_data('db/digital_pulpit.db', video_id)
        if data:
            write_audit_file(video_id, data, before, after, "doctrine_vs_experience", i, out_dir)

    print("=" * 80)
    print(f"AUDIT COMPLETE: 15 files written to {out_dir}/")
    print("=" * 80)


if __name__ == "__main__":
    main()
