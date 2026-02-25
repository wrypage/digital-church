#!/usr/bin/env python3
"""
calibration_report.py

Generate comprehensive report on calibration patch results.
"""

import sqlite3
from typing import Dict, List, Tuple


def get_brain_results(db_path: str) -> List[Dict]:
    """Get all brain results."""
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    query = """
        SELECT video_id, grace_vs_effort, hope_vs_fear,
               doctrine_vs_experience, scripture_vs_story, theological_density
        FROM brain_results
    """

    results = [dict(row) for row in con.execute(query)]
    con.close()
    return results


def binned_distribution(scores: List[float], axis_name: str) -> Dict:
    """Calculate distribution across bins."""
    bins = [
        (1.0, 1.0, "Exactly 1.000"),
        (0.5, 0.999, "0.5 to 0.999"),
        (0.05, 0.499, "0.05 to 0.5"),
        (-0.05, 0.049, "~0 (neutral)"),
        (-0.5, -0.051, "-0.05 to -0.5"),
        (-0.999, -0.501, "-0.5 to -0.999"),
        (-1.0, -1.0, "Exactly -1.000")
    ]

    counts = {label: 0 for _, _, label in bins}

    for score in scores:
        for low, high, label in bins:
            if low == high:  # Exact match
                if abs(score - low) < 0.0001:
                    counts[label] += 1
                    break
            else:  # Range
                if low <= score <= high:
                    counts[label] += 1
                    break

    return counts


def main():
    results = get_brain_results('db/digital_pulpit.db')
    total = len(results)

    print("=" * 80)
    print("CALIBRATION PATCH REPORT")
    print("=" * 80)
    print()
    print("Changes:")
    print("  1. Modified score_axes() with AXIS_INERTIA_K=2.0, AXIS_MIN_ACTIVATION=3.0")
    print("  2. Removed 'peace' and 'rest' from hope keywords")
    print("  3. Config v5.4 (final)")
    print()
    print("=" * 80)
    print()

    # 1. Hope distribution
    print("1. HOPE_VS_FEAR DISTRIBUTION")
    print("-" * 80)

    hope_scores = [r['hope_vs_fear'] for r in results]
    hope_dist = binned_distribution(hope_scores, 'hope_vs_fear')

    print(f"{'Bucket':<20} {'Count':<10} {'Percentage':<15}")
    print("-" * 80)

    for label, count in hope_dist.items():
        pct = count / total * 100
        print(f"{label:<20} {count:<10} {pct:<14.1f}%")

    print()

    # 2. Corpus-wide averages
    print("2. CORPUS-WIDE AVERAGES")
    print("-" * 80)

    avg_gve = sum(r['grace_vs_effort'] for r in results) / total
    avg_hvf = sum(r['hope_vs_fear'] for r in results) / total
    avg_dve = sum(r['doctrine_vs_experience'] for r in results) / total
    avg_svs = sum(r['scripture_vs_story'] for r in results) / total

    print(f"{'Axis':<30} {'Average':<15}")
    print("-" * 80)
    print(f"{'grace_vs_effort':<30} {avg_gve:<14.3f}")
    print(f"{'hope_vs_fear':<30} {avg_hvf:<14.3f}")
    print(f"{'doctrine_vs_experience':<30} {avg_dve:<14.3f}")
    print(f"{'scripture_vs_story':<30} {avg_svs:<14.3f}")
    print()

    # 3. 1 Timothy
    print("3. 1 TIMOTHY TEST CASE (78db72267e74fa70)")
    print("-" * 80)

    timothy = next((r for r in results if r['video_id'] == '78db72267e74fa70'), None)
    if timothy:
        print(f"{'Axis':<30} {'Score':<15}")
        print("-" * 80)
        print(f"{'grace_vs_effort':<30} {timothy['grace_vs_effort']:<14.3f}")
        print(f"{'hope_vs_fear':<30} {timothy['hope_vs_fear']:<14.3f}")
        print(f"{'doctrine_vs_experience':<30} {timothy['doctrine_vs_experience']:<14.3f}")
        print(f"{'scripture_vs_story':<30} {timothy['scripture_vs_story']:<14.3f}")
        print(f"{'theological_density':<30} {timothy['theological_density']:<14.1f}")
    else:
        print("Not found")

    print()

    # 4. Perfect scores
    print("4. PERCENTAGE AT EXACTLY ±1.000 ON HOPE_VS_FEAR")
    print("-" * 80)

    perfect_pos = sum(1 for r in results if abs(r['hope_vs_fear'] - 1.0) < 0.0001)
    perfect_neg = sum(1 for r in results if abs(r['hope_vs_fear'] - (-1.0)) < 0.0001)
    perfect_total = perfect_pos + perfect_neg

    print(f"Exactly +1.000:  {perfect_pos} ({perfect_pos/total*100:.1f}%)")
    print(f"Exactly -1.000:  {perfect_neg} ({perfect_neg/total*100:.1f}%)")
    print(f"Total at ±1.000: {perfect_total} ({perfect_total/total*100:.1f}%)")
    print()

    # 5. Grace distribution
    print("5. GRACE_VS_EFFORT DISTRIBUTION (for comparison)")
    print("-" * 80)

    grace_scores = [r['grace_vs_effort'] for r in results]
    grace_dist = binned_distribution(grace_scores, 'grace_vs_effort')

    print(f"{'Bucket':<20} {'Count':<10} {'Percentage':<15}")
    print("-" * 80)

    for label, count in grace_dist.items():
        pct = count / total * 100
        print(f"{label:<20} {count:<10} {pct:<14.1f}%")

    print()

    perfect_grace_pos = sum(1 for r in results if abs(r['grace_vs_effort'] - 1.0) < 0.0001)
    perfect_grace_neg = sum(1 for r in results if abs(r['grace_vs_effort'] - (-1.0)) < 0.0001)
    perfect_grace_total = perfect_grace_pos + perfect_grace_neg

    print(f"Grace at exactly ±1.000: {perfect_grace_total} ({perfect_grace_total/total*100:.1f}%)")
    print()

    # 6. Theological density
    print("6. THEOLOGICAL DENSITY AVERAGE")
    print("-" * 80)

    avg_density = sum(r['theological_density'] for r in results) / total
    print(f"Average: {avg_density:.1f} (should be unchanged)")
    print()

    print("=" * 80)
    print("INTERPRETATION")
    print("=" * 80)
    print()

    # Check if calibration worked
    if perfect_total < total * 0.10:
        print("✓ Ceiling saturation FIXED: <10% at ±1.000")
    else:
        print("⚠ Still seeing saturation: >10% at ±1.000")

    if 0.3 < avg_hvf < 0.7:
        print("✓ Hope axis well-centered")
    else:
        print(f"⚠ Hope axis shifted: {avg_hvf:.3f}")

    if abs(avg_density - 89.0) < 5.0:
        print("✓ Theological density unchanged (as expected)")
    else:
        print(f"⚠ Density changed unexpectedly: {avg_density:.1f}")

    print()
    print("=" * 80)


if __name__ == "__main__":
    main()
