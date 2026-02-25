#!/usr/bin/env python3
"""
generate_corpus_report.py

Compare brain_results before and after v2.1 corpus regeneration.
Report axis sign changes and corpus-wide statistics.
"""

import sqlite3
import sys
from typing import Dict, Tuple


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
                    'theological_density': float(parts[5])
                }
    return baseline


def get_current_results(db_path: str) -> Dict[str, Dict[str, float]]:
    """Get current brain_results from database."""
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


def sign(x: float) -> int:
    """Return sign of number (-1, 0, 1)."""
    if x > 0.05:
        return 1
    elif x < -0.05:
        return -1
    else:
        return 0


def main():
    baseline = load_baseline('out/brain_results_before_v2_1.csv')
    current = get_current_results('db/digital_pulpit.db')

    print("=" * 80)
    print("CORPUS REGENERATION REPORT: Summary Generator v2.1 + Config v5.3")
    print("=" * 80)
    print()

    # Track changes
    gve_flips = []
    hvf_flips = []
    dve_flips = []
    svs_flips = []

    total_compared = 0

    for video_id in baseline.keys():
        if video_id not in current:
            continue

        total_compared += 1

        b = baseline[video_id]
        c = current[video_id]

        # Check for sign flips on each axis
        if sign(b['grace_vs_effort']) != sign(c['grace_vs_effort']):
            gve_flips.append((video_id, b['grace_vs_effort'], c['grace_vs_effort']))

        if sign(b['hope_vs_fear']) != sign(c['hope_vs_fear']):
            hvf_flips.append((video_id, b['hope_vs_fear'], c['hope_vs_fear']))

        if sign(b['doctrine_vs_experience']) != sign(c['doctrine_vs_experience']):
            dve_flips.append((video_id, b['doctrine_vs_experience'], c['doctrine_vs_experience']))

        if sign(b['scripture_vs_story']) != sign(c['scripture_vs_story']):
            svs_flips.append((video_id, b['scripture_vs_story'], c['scripture_vs_story']))

    # Corpus-wide averages
    avg_gve_before = sum(baseline[vid]['grace_vs_effort'] for vid in baseline) / len(baseline)
    avg_gve_after = sum(current[vid]['grace_vs_effort'] for vid in current) / len(current)

    avg_hvf_before = sum(baseline[vid]['hope_vs_fear'] for vid in baseline) / len(baseline)
    avg_hvf_after = sum(current[vid]['hope_vs_fear'] for vid in current) / len(current)

    avg_dve_before = sum(baseline[vid]['doctrine_vs_experience'] for vid in baseline) / len(baseline)
    avg_dve_after = sum(current[vid]['doctrine_vs_experience'] for vid in current) / len(current)

    avg_svs_before = sum(baseline[vid]['scripture_vs_story'] for vid in baseline) / len(baseline)
    avg_svs_after = sum(current[vid]['scripture_vs_story'] for vid in current) / len(current)

    avg_density_before = sum(baseline[vid]['theological_density'] for vid in baseline) / len(baseline)
    avg_density_after = sum(current[vid]['theological_density'] for vid in current) / len(current)

    # Print report
    print(f"SUMMARY STATISTICS")
    print(f"-" * 80)
    print(f"Sermons in baseline:        {len(baseline)}")
    print(f"Sermons in current:         {len(current)}")
    print(f"Sermons compared:           {total_compared}")
    print()

    print(f"AXIS SIGN CHANGES (threshold ±0.05)")
    print(f"-" * 80)
    print(f"grace_vs_effort flips:      {len(gve_flips)}")
    print(f"hope_vs_fear flips:         {len(hvf_flips)}")
    print(f"doctrine_vs_experience:     {len(dve_flips)}")
    print(f"scripture_vs_story:         {len(svs_flips)}")
    print(f"TOTAL FLIPS:                {len(gve_flips) + len(hvf_flips) + len(dve_flips) + len(svs_flips)}")
    print()

    print(f"CORPUS-WIDE AVERAGES")
    print(f"-" * 80)
    print(f"{'Axis':<25} {'Before':<12} {'After':<12} {'Change':<12}")
    print(f"-" * 80)
    print(f"{'grace_vs_effort':<25} {avg_gve_before:>11.3f} {avg_gve_after:>11.3f} {avg_gve_after - avg_gve_before:>+11.3f}")
    print(f"{'hope_vs_fear':<25} {avg_hvf_before:>11.3f} {avg_hvf_after:>11.3f} {avg_hvf_after - avg_hvf_before:>+11.3f}")
    print(f"{'doctrine_vs_experience':<25} {avg_dve_before:>11.3f} {avg_dve_after:>11.3f} {avg_dve_after - avg_dve_before:>+11.3f}")
    print(f"{'scripture_vs_story':<25} {avg_svs_before:>11.3f} {avg_svs_after:>11.3f} {avg_svs_after - avg_svs_before:>+11.3f}")
    print(f"{'theological_density':<25} {avg_density_before:>11.1f} {avg_density_after:>11.1f} {avg_density_after - avg_density_before:>+11.1f}")
    print()

    # Show largest changes
    if hvf_flips:
        print(f"HOPE_VS_FEAR SIGN FLIPS ({len(hvf_flips)} total)")
        print(f"-" * 80)
        print(f"{'Video ID':<20} {'Before':>10} {'After':>10} {'Change':>12}")
        print(f"-" * 80)
        for vid, before, after in sorted(hvf_flips, key=lambda x: abs(x[2] - x[1]), reverse=True)[:20]:
            print(f"{vid:<20} {before:>10.3f} {after:>10.3f} {after - before:>+12.3f}")
        if len(hvf_flips) > 20:
            print(f"... and {len(hvf_flips) - 20} more")
        print()

    if gve_flips:
        print(f"GRACE_VS_EFFORT SIGN FLIPS ({len(gve_flips)} total)")
        print(f"-" * 80)
        print(f"{'Video ID':<20} {'Before':>10} {'After':>10} {'Change':>12}")
        print(f"-" * 80)
        for vid, before, after in sorted(gve_flips, key=lambda x: abs(x[2] - x[1]), reverse=True)[:20]:
            print(f"{vid:<20} {before:>10.3f} {after:>10.3f} {after - before:>+12.3f}")
        if len(gve_flips) > 20:
            print(f"... and {len(gve_flips) - 20} more")
        print()

    print(f"INTERPRETATION")
    print(f"-" * 80)

    if avg_hvf_after > avg_hvf_before:
        print(f"✓ hope_vs_fear INCREASED by {avg_hvf_after - avg_hvf_before:+.3f}")
        print(f"  Summary Generator v2.1 successfully captured more hope language")

    if avg_gve_after > avg_gve_before:
        print(f"✓ grace_vs_effort INCREASED by {avg_gve_after - avg_gve_before:+.3f}")
        print(f"  Summary Generator v2.1 successfully captured more grace language")

    if avg_density_after > avg_density_before:
        print(f"✓ theological_density INCREASED by {avg_density_after - avg_density_before:+.1f}")
        print(f"  Config v5.3 expanded keyword lists captured more theological content")

    print()
    print("=" * 80)
    print("REGENERATION COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()
