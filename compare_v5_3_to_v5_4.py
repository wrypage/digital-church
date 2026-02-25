#!/usr/bin/env python3
"""
compare_v5_3_to_v5_4.py

Compare Brain results between config v5.3 and v5.4 (removed "peace" from hope keywords).
"""

import sqlite3
from typing import Dict


def load_baseline(csv_path: str) -> Dict[str, Dict[str, float]]:
    """Load v5.3 brain_results from CSV."""
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
                    'theological_density': float(parts[5])
                }
    return baseline


def get_current_results(db_path: str) -> Dict[str, Dict[str, float]]:
    """Get current brain_results from database (v5.4)."""
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    query = """
        SELECT video_id, grace_vs_effort, hope_vs_fear,
               doctrine_vs_experience, scripture_vs_story, theological_density
        FROM brain_results
    """

    current = {}
    for row in con.execute(query):
        current[row['video_id']] = {
            'grace_vs_effort': row['grace_vs_effort'],
            'hope_vs_fear': row['hope_vs_fear'],
            'doctrine_vs_experience': row['doctrine_vs_experience'],
            'scripture_vs_story': row['scripture_vs_story'],
            'theological_density': row['theological_density']
        }

    con.close()
    return current


def main():
    v5_3 = load_baseline('out/brain_results_v5_3.csv')
    v5_4 = get_current_results('db/digital_pulpit.db')

    print("=" * 80)
    print("CONFIG v5.3 → v5.4 COMPARISON")
    print("=" * 80)
    print()
    print("Change: Removed 'peace' from hope keywords")
    print()

    # 1. Count perfect hope scores
    perfect_hope_v5_3 = sum(1 for vid in v5_3 if v5_3[vid]['hope_vs_fear'] >= 0.999)
    perfect_hope_v5_4 = sum(1 for vid in v5_4 if v5_4[vid]['hope_vs_fear'] >= 0.999)

    print(f"1. PERFECT HOPE SCORES (hope_vs_fear = 1.000)")
    print(f"   v5.3: {perfect_hope_v5_3} sermons ({perfect_hope_v5_3/len(v5_3)*100:.1f}%)")
    print(f"   v5.4: {perfect_hope_v5_4} sermons ({perfect_hope_v5_4/len(v5_4)*100:.1f}%)")
    print(f"   Change: {perfect_hope_v5_4 - perfect_hope_v5_3:+d} sermons")
    print()

    # 2. Corpus-wide average
    avg_hvf_v5_3 = sum(v5_3[vid]['hope_vs_fear'] for vid in v5_3) / len(v5_3)
    avg_hvf_v5_4 = sum(v5_4[vid]['hope_vs_fear'] for vid in v5_4) / len(v5_4)

    print(f"2. CORPUS-WIDE AVERAGE (hope_vs_fear)")
    print(f"   v5.3: {avg_hvf_v5_3:.3f}")
    print(f"   v5.4: {avg_hvf_v5_4:.3f}")
    print(f"   Change: {avg_hvf_v5_4 - avg_hvf_v5_3:+.3f}")
    print()

    # 3. Count changed scores
    changed = 0
    for vid in v5_3:
        if vid in v5_4:
            if abs(v5_3[vid]['hope_vs_fear'] - v5_4[vid]['hope_vs_fear']) > 0.001:
                changed += 1

    print(f"3. SERMONS WITH CHANGED HOPE_VS_FEAR SCORES")
    print(f"   Changed: {changed} sermons ({changed/len(v5_3)*100:.1f}%)")
    print(f"   Unchanged: {len(v5_3) - changed} sermons")
    print()

    # 4. Check 1 Timothy
    timothy_id = '78db72267e74fa70'
    if timothy_id in v5_3 and timothy_id in v5_4:
        hvf_v5_3 = v5_3[timothy_id]['hope_vs_fear']
        hvf_v5_4 = v5_4[timothy_id]['hope_vs_fear']

        print(f"4. 1 TIMOTHY TEST CASE (78db72267e74fa70)")
        print(f"   v5.3: hope_vs_fear = {hvf_v5_3:.3f}")
        print(f"   v5.4: hope_vs_fear = {hvf_v5_4:.3f}")
        print(f"   Change: {hvf_v5_4 - hvf_v5_3:+.3f}")

        if hvf_v5_4 > 0:
            print(f"   ✓ Still positive (> 0)")
        else:
            print(f"   ✗ WARNING: Dropped to neutral/negative")
    else:
        print(f"4. 1 TIMOTHY TEST CASE: Not found")

    print()
    print("=" * 80)

    # Additional analysis: show distribution
    print()
    print("HOPE_VS_FEAR DISTRIBUTION")
    print("-" * 80)

    # Bin the scores
    bins = [
        (-1.5, -0.75, "Strong Fear"),
        (-0.75, -0.25, "Moderate Fear"),
        (-0.25, 0.25, "Neutral"),
        (0.25, 0.75, "Moderate Hope"),
        (0.75, 1.5, "Strong Hope")
    ]

    print(f"{'Range':<20} {'v5.3':<10} {'v5.4':<10} {'Change':<10}")
    print("-" * 80)

    for low, high, label in bins:
        count_v5_3 = sum(1 for vid in v5_3 if low <= v5_3[vid]['hope_vs_fear'] < high)
        count_v5_4 = sum(1 for vid in v5_4 if low <= v5_4[vid]['hope_vs_fear'] < high)
        change = count_v5_4 - count_v5_3

        pct_v5_3 = count_v5_3 / len(v5_3) * 100
        pct_v5_4 = count_v5_4 / len(v5_4) * 100

        print(f"{label:<20} {count_v5_3:>3} ({pct_v5_3:>4.1f}%) {count_v5_4:>3} ({pct_v5_4:>4.1f}%) {change:>+3}")

    print()
    print("=" * 80)


if __name__ == "__main__":
    main()
