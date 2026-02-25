#!/usr/bin/env python3
"""
generate_spot_check.py

Quality control spot check for corpus regeneration.
"""

import sqlite3
import json
from datetime import datetime
from typing import Dict, Any, List, Tuple


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


def get_video_full_data(db_path: str, video_id: str) -> Dict[str, Any]:
    """Get complete video data including raw_scores_json."""
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    query = """
        SELECT v.title, c.channel_name,
               t.summary_text,
               br.grace_vs_effort, br.hope_vs_fear,
               br.doctrine_vs_experience, br.scripture_vs_story,
               br.theological_density, br.raw_scores_json
        FROM videos v
        JOIN transcripts t ON v.video_id = t.video_id
        LEFT JOIN channels c ON v.channel_id = c.channel_id
        JOIN brain_results br ON v.video_id = br.video_id
        WHERE v.video_id = ?
    """

    row = con.execute(query, (video_id,)).fetchone()
    con.close()

    if not row:
        return None

    raw_scores = json.loads(row['raw_scores_json']) if row['raw_scores_json'] else {}

    return {
        'title': row['title'],
        'channel_name': row['channel_name'] or 'Unknown',
        'summary': row['summary_text'],
        'grace_vs_effort': row['grace_vs_effort'],
        'hope_vs_fear': row['hope_vs_fear'],
        'doctrine_vs_experience': row['doctrine_vs_experience'],
        'scripture_vs_story': row['scripture_vs_story'],
        'theological_density': row['theological_density'],
        'raw_scores': raw_scores
    }


def get_aggregate_stats(db_path: str, baseline: Dict) -> Dict[str, Any]:
    """Get aggregate statistics for flags."""
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    # Count perfect hope scores
    hope_perfect = con.execute("""
        SELECT COUNT(*) FROM brain_results WHERE hope_vs_fear >= 0.999
    """).fetchone()[0]

    # Count perfect grace scores
    grace_perfect = con.execute("""
        SELECT COUNT(*) FROM brain_results WHERE grace_vs_effort >= 0.999
    """).fetchone()[0]

    # Count high density
    high_density = con.execute("""
        SELECT COUNT(*) FROM brain_results WHERE theological_density > 150
    """).fetchone()[0]

    con.close()

    return {
        'hope_perfect': hope_perfect,
        'grace_perfect': grace_perfect,
        'high_density': high_density,
        'total': len(baseline)
    }


def find_doctrine_extremes(db_path: str, baseline: Dict) -> Tuple[str, str]:
    """Find sermons with largest doctrine_vs_experience changes."""
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    query = """
        SELECT video_id, doctrine_vs_experience
        FROM brain_results
    """

    current = {row['video_id']: row['doctrine_vs_experience']
               for row in con.execute(query)}
    con.close()

    # Calculate changes
    changes = []
    for vid in baseline:
        if vid in current:
            old_val = baseline[vid]['doctrine_vs_experience']
            new_val = current[vid]
            change = new_val - old_val
            changes.append((vid, old_val, new_val, change))

    # Sort by change
    changes.sort(key=lambda x: x[3])

    # Most negative (toward experience)
    most_negative = changes[0][0] if changes else None

    # Most positive (toward doctrine)
    most_positive = changes[-1][0] if changes else None

    return most_positive, most_negative


def format_sermon_comparison(video_id: str, baseline_scores: Dict,
                             current_data: Dict) -> str:
    """Format a sermon comparison section."""
    output = []

    output.append(f"**Video ID:** {video_id}")
    output.append(f"**Channel:** {current_data['channel_name']}")
    output.append(f"**Title:** {current_data['title']}")
    output.append("")

    # Axis scores comparison
    output.append("### Axis Scores")
    output.append("")
    output.append("| Axis | Before | After | Change |")
    output.append("|------|--------|-------|--------|")

    for axis in ['grace_vs_effort', 'hope_vs_fear', 'doctrine_vs_experience',
                 'scripture_vs_story', 'theological_density']:
        old_val = baseline_scores.get(axis, 0.0)
        new_val = current_data.get(axis, 0.0)
        change = new_val - old_val
        output.append(f"| {axis} | {old_val:.3f} | {new_val:.3f} | {change:+.3f} |")

    output.append("")

    # Category counts
    raw_scores = current_data.get('raw_scores', {})
    category_counts = raw_scores.get('category_counts', {})

    output.append("### Category Counts (Current)")
    output.append("")
    output.append("| Category | Count |")
    output.append("|----------|-------|")
    for cat in ['grace', 'effort', 'hope', 'fear', 'doctrine', 'experience']:
        count = category_counts.get(cat, 0)
        output.append(f"| {cat} | {count} |")
    output.append("")

    # Summary
    output.append("### Summary (v2.1)")
    output.append("")
    output.append("```")
    output.append(current_data['summary'] or '[No summary]')
    output.append("```")
    output.append("")

    # Burden sentence
    intent_vectors = raw_scores.get('intent_vectors', {})
    burden = intent_vectors.get('primary_burden_summary', '[Not found]')
    output.append(f"**Burden:** {burden}")
    output.append("")

    return "\n".join(output)


def main():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"out/experiments/spot_check_{timestamp}.md"

    baseline = load_baseline('out/brain_results_before_v2_1.csv')
    db_path = 'db/digital_pulpit.db'

    output = []

    # Header
    output.append("# Corpus Regeneration Spot Check Report")
    output.append("")
    output.append(f"**Generated:** {datetime.now().isoformat()}")
    output.append(f"**Version:** v2.1-5.3")
    output.append("")
    output.append("---")
    output.append("")

    # SECTION 1: RED FLAG SERMON
    output.append("## SECTION 1: THE RED FLAG SERMON")
    output.append("")
    output.append("**Investigation:** b0dbb678a79afc36")
    output.append("")
    output.append("This sermon showed a concerning change: hope_vs_fear dropped from 1.000 to -0.333,")
    output.append("a complete reversal. Does the new summary introduce warning language disproportionate")
    output.append("to the sermon's apparent tone?")
    output.append("")

    red_flag_id = 'b0dbb678a79afc36'
    red_flag_data = get_video_full_data(db_path, red_flag_id)
    red_flag_baseline = baseline.get(red_flag_id, {})

    if red_flag_data:
        output.append(format_sermon_comparison(red_flag_id, red_flag_baseline, red_flag_data))

        # Assessment
        output.append("### Assessment")
        output.append("")

        raw_scores = red_flag_data.get('raw_scores', {})
        cat_counts = raw_scores.get('category_counts', {})
        hope_count = cat_counts.get('hope', 0)
        fear_count = cat_counts.get('fear', 0)

        output.append(f"- **Hope keywords:** {hope_count}")
        output.append(f"- **Fear keywords:** {fear_count}")
        output.append(f"- **Old score:** hope_vs_fear = {red_flag_baseline.get('hope_vs_fear', 0):.3f}")
        output.append(f"- **New score:** hope_vs_fear = {red_flag_data['hope_vs_fear']:.3f}")
        output.append("")

        if fear_count > hope_count:
            output.append("⚠️ **FINDING:** New summary contains more fear than hope keywords.")
            output.append("This warrants manual review to determine if the summary accurately reflects")
            output.append("the sermon's actual tone or if v2.1 over-indexed on warning language.")
        else:
            output.append("✓ **FINDING:** Fear/hope ratio seems reasonable given the counts.")
        output.append("")
    else:
        output.append("❌ Video data not found.")
        output.append("")

    output.append("---")
    output.append("")

    # SECTION 2: GRACE SPOT CHECKS
    output.append("## SECTION 2: GRACE_VS_EFFORT SPOT CHECKS")
    output.append("")

    grace_cases = [
        ('8519693923b7a623', 'A) Large Positive Flip (-1.000 → +1.000)'),
        ('9a582104efe73970', 'B) Large Negative Flip (+1.000 → -1.000)'),
        ('fc375931a3d0898a', 'C) Moderate Flip (-0.500 → +0.750)')
    ]

    for video_id, label in grace_cases:
        output.append(f"### {label}")
        output.append("")

        data = get_video_full_data(db_path, video_id)
        base = baseline.get(video_id, {})

        if data:
            output.append(format_sermon_comparison(video_id, base, data))
        else:
            output.append("❌ Video data not found.")
            output.append("")

        output.append("---")
        output.append("")

    # SECTION 3: DOCTRINE SPOT CHECKS
    output.append("## SECTION 3: DOCTRINE_VS_EXPERIENCE SPOT CHECKS")
    output.append("")

    most_doctrinal, most_experiential = find_doctrine_extremes(db_path, baseline)

    doctrine_cases = [
        (most_doctrinal, 'Most Doctrinal Shift'),
        (most_experiential, 'Most Experiential Shift')
    ]

    for video_id, label in doctrine_cases:
        if not video_id:
            continue

        output.append(f"### {label}")
        output.append("")

        data = get_video_full_data(db_path, video_id)
        base = baseline.get(video_id, {})

        if data:
            output.append(format_sermon_comparison(video_id, base, data))
        else:
            output.append("❌ Video data not found.")
            output.append("")

        output.append("---")
        output.append("")

    # SECTION 4: BURDEN QUALITY
    output.append("## SECTION 4: SUMMARY QUALITY ASSESSMENT")
    output.append("")
    output.append("**Note:** Old summaries were not backed up before regeneration.")
    output.append("Old burden sentences are not available for comparison.")
    output.append("")
    output.append("### New Burden Sentences")
    output.append("")
    output.append("| Video ID | Sermon Title | New Burden |")
    output.append("|----------|--------------|------------|")

    all_check_ids = [red_flag_id] + [vid for vid, _ in grace_cases] + \
                    [vid for vid, _ in doctrine_cases if vid]

    for video_id in all_check_ids:
        data = get_video_full_data(db_path, video_id)
        if data:
            raw_scores = data.get('raw_scores', {})
            intent_vectors = raw_scores.get('intent_vectors', {})
            burden = intent_vectors.get('primary_burden_summary', '[Not found]')
            title = data['title'][:40] + '...' if len(data['title']) > 40 else data['title']
            output.append(f"| {video_id[:16]}... | {title} | {burden} |")

    output.append("")
    output.append("---")
    output.append("")

    # SECTION 5: AGGREGATE FLAGS
    output.append("## SECTION 5: AGGREGATE FLAGS")
    output.append("")

    stats = get_aggregate_stats(db_path, baseline)
    total = stats['total']

    output.append(f"### Perfect Hope Scores (hope_vs_fear = 1.000)")
    output.append(f"- **Count:** {stats['hope_perfect']}")
    output.append(f"- **Percentage:** {stats['hope_perfect']/total*100:.1f}%")
    output.append("")

    if stats['hope_perfect'] > total * 0.15:
        output.append("⚠️ **FLAG:** More than 15% of sermons have perfect hope scores.")
        output.append("This may indicate over-indexing on hope keywords.")
    else:
        output.append("✓ Reasonable distribution.")
    output.append("")

    output.append(f"### Perfect Grace Scores (grace_vs_effort = 1.000)")
    output.append(f"- **Count:** {stats['grace_perfect']}")
    output.append(f"- **Percentage:** {stats['grace_perfect']/total*100:.1f}%")
    output.append("")

    if stats['grace_perfect'] > total * 0.15:
        output.append("⚠️ **FLAG:** More than 15% of sermons have perfect grace scores.")
        output.append("This may indicate over-indexing on grace keywords.")
    else:
        output.append("✓ Reasonable distribution.")
    output.append("")

    output.append(f"### High Theological Density (>150)")
    output.append(f"- **Count:** {stats['high_density']}")
    output.append(f"- **Percentage:** {stats['high_density']/total*100:.1f}%")
    output.append("")

    if stats['high_density'] > total * 0.10:
        output.append("⚠️ **FLAG:** More than 10% of sermons have density > 150.")
        output.append("This may indicate format inflation or over-counting.")
    else:
        output.append("✓ Reasonable distribution.")
    output.append("")

    output.append("---")
    output.append("")

    # CONCLUSION
    output.append("## CONCLUSION")
    output.append("")
    output.append("This spot check examined 6 sermons in detail:")
    output.append("- 1 red flag (hope reversal)")
    output.append("- 3 grace flips (positive, negative, moderate)")
    output.append("- 2 doctrine extremes")
    output.append("")
    output.append("**Recommendations:**")
    output.append("1. Manually review the red flag sermon (b0dbb678a79afc36)")
    output.append("2. Check for false positives in perfect scores (if flagged)")
    output.append("3. Validate high-density sermons (if flagged)")
    output.append("4. Consider backing up summaries before future regenerations")
    output.append("")
    output.append("---")
    output.append("")
    output.append(f"**Report generated:** {datetime.now().isoformat()}")

    # Write to file
    with open(output_file, 'w') as f:
        f.write("\n".join(output))

    print(f"✓ Spot check report written to: {output_file}")
    return output_file


if __name__ == "__main__":
    main()
