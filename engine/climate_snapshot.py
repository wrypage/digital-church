#!/usr/bin/env python3
"""
engine/climate_snapshot.py - Climate-First Reporting

Compares current 30 days vs previous 30 days across the sermon corpus.
Focuses on theological climate rather than drift anomalies.
"""

import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from .config import DATABASE_PATH


def _connect(db_path: str = DATABASE_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _safe_json_load(s: str, default):
    if not s:
        return default
    try:
        import json
        return json.loads(s)
    except Exception:
        return default


def fetch_period_data(conn: sqlite3.Connection, start_date: str, end_date: str) -> List[Dict]:
    """Fetch brain_results for a date range."""
    q = """
    SELECT
        br.video_id,
        br.theological_density,
        br.grace_vs_effort,
        br.hope_vs_fear,
        br.doctrine_vs_experience,
        br.scripture_vs_story,
        br.raw_scores_json,
        v.published_at
    FROM brain_results br
    JOIN videos v ON br.video_id = v.video_id
    WHERE v.published_at >= ? AND v.published_at < ?
    """

    rows = conn.execute(q, (start_date, end_date)).fetchall()

    items = []
    for r in rows:
        raw = _safe_json_load(r['raw_scores_json'], {})
        drift_level = raw.get('drift_level', 'unknown')

        items.append({
            'video_id': r['video_id'],
            'published_at': r['published_at'],
            'theological_density': float(r['theological_density'] or 0.0),
            'grace_vs_effort': float(r['grace_vs_effort'] or 0.0),
            'hope_vs_fear': float(r['hope_vs_fear'] or 0.0),
            'doctrine_vs_experience': float(r['doctrine_vs_experience'] or 0.0),
            'scripture_vs_story': float(r['scripture_vs_story'] or 0.0),
            'drift_level': drift_level,
            'category_density': raw.get('category_density', {}),
            'scripture_refs': raw.get('scripture_refs', {}),
        })

    return items


def compute_climate_stats(items: List[Dict]) -> Dict:
    """Compute aggregate stats for a period."""
    if not items:
        return {
            'count': 0,
            'avg_density': 0.0,
            'avg_axes': {},
            'top_categories': [],
            'top_books': [],
            'drift_distribution': {},
        }

    # Average axes
    avg_axes = {
        'grace_vs_effort': sum(i['grace_vs_effort'] for i in items) / len(items),
        'hope_vs_fear': sum(i['hope_vs_fear'] for i in items) / len(items),
        'doctrine_vs_experience': sum(i['doctrine_vs_experience'] for i in items) / len(items),
        'scripture_vs_story': sum(i['scripture_vs_story'] for i in items) / len(items),
    }

    # Average density
    avg_density = sum(i['theological_density'] for i in items) / len(items)

    # Aggregate categories
    category_totals = {}
    for item in items:
        for cat, val in item['category_density'].items():
            category_totals[cat] = category_totals.get(cat, 0.0) + float(val)

    top_categories = sorted(category_totals.items(), key=lambda x: x[1], reverse=True)[:5]

    # Aggregate scripture refs
    book_totals = {}
    for item in items:
        for book, count in item['scripture_refs'].items():
            book_totals[book] = book_totals.get(book, 0) + count

    top_books = sorted(book_totals.items(), key=lambda x: x[1], reverse=True)[:5]

    # Drift distribution
    drift_distribution = {}
    for item in items:
        level = item['drift_level']
        drift_distribution[level] = drift_distribution.get(level, 0) + 1

    return {
        'count': len(items),
        'avg_density': avg_density,
        'avg_axes': avg_axes,
        'top_categories': top_categories,
        'top_books': top_books,
        'drift_distribution': drift_distribution,
    }


def generate_climate_snapshot(days: int = 30) -> Dict:
    """Generate climate snapshot comparing current vs previous period."""
    conn = _connect()

    now = datetime.utcnow()
    current_end = now.strftime('%Y-%m-%d %H:%M:%S')
    current_start = (now - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
    previous_end = current_start
    previous_start = (now - timedelta(days=days * 2)).strftime('%Y-%m-%d %H:%M:%S')

    current_items = fetch_period_data(conn, current_start, current_end)
    previous_items = fetch_period_data(conn, previous_start, previous_end)

    current_stats = compute_climate_stats(current_items)
    previous_stats = compute_climate_stats(previous_items)

    # Compute deltas
    deltas = {}
    if previous_stats['count'] > 0:
        deltas['density'] = current_stats['avg_density'] - previous_stats['avg_density']
        deltas['axes'] = {
            axis: current_stats['avg_axes'][axis] - previous_stats['avg_axes'][axis]
            for axis in current_stats['avg_axes']
        }

    # Drift rate (percentage with drift)
    drift_rate_current = 0.0
    if current_stats['count'] > 0:
        drift_count = sum(
            current_stats['drift_distribution'].get(level, 0)
            for level in ['anomaly', 'strong_shift', 'moderate_shift']
        )
        drift_rate_current = (drift_count / current_stats['count']) * 100

    drift_rate_previous = 0.0
    if previous_stats['count'] > 0:
        drift_count_prev = sum(
            previous_stats['drift_distribution'].get(level, 0)
            for level in ['anomaly', 'strong_shift', 'moderate_shift']
        )
        drift_rate_previous = (drift_count_prev / previous_stats['count']) * 100

    conn.close()

    return {
        'period_days': days,
        'current': current_stats,
        'previous': previous_stats,
        'deltas': deltas,
        'drift_rate': {
            'current': drift_rate_current,
            'previous': drift_rate_previous,
            'delta': drift_rate_current - drift_rate_previous,
        },
        'generated_at': datetime.utcnow().isoformat() + 'Z',
    }


def print_climate_snapshot(days: int = 30):
    """Print human-readable climate snapshot."""
    snapshot = generate_climate_snapshot(days=days)

    print("=" * 80)
    print(f"CLIMATE SNAPSHOT: Last {days} Days vs Previous {days} Days")
    print("=" * 80)
    print()

    current = snapshot['current']
    previous = snapshot['previous']
    deltas = snapshot['deltas']

    # Sermon counts
    print(f"Sermon Count:")
    print(f"  Current:  {current['count']}")
    print(f"  Previous: {previous['count']}")
    print(f"  Delta:    {current['count'] - previous['count']:+d}")
    print()

    # Average theological density
    print(f"Avg Theological Density (per 1000 words):")
    print(f"  Current:  {current['avg_density']:.2f}")
    print(f"  Previous: {previous['avg_density']:.2f}")
    if deltas:
        print(f"  Delta:    {deltas['density']:+.2f}")
    print()

    # Average axes
    print("Avg Axis Scores (range -1 to +1):")
    for axis, val in current['avg_axes'].items():
        prev_val = previous['avg_axes'].get(axis, 0.0)
        delta_val = deltas.get('axes', {}).get(axis, 0.0)
        axis_label = axis.replace('_', ' ').title()
        print(f"  {axis_label:30s}: {val:+.3f} (prev: {prev_val:+.3f}, delta: {delta_val:+.3f})")
    print()

    # Top categories
    print("Top 5 Categories (current period):")
    for i, (cat, density) in enumerate(current['top_categories'], 1):
        print(f"  {i}. {cat:30s}: {density:.1f}")
    print()

    # Top books
    print("Top 5 Scripture Books (current period):")
    for i, (book, count) in enumerate(current['top_books'], 1):
        print(f"  {i}. {book:30s}: {count} references")
    print()

    # Drift rate (included but not central)
    print("Drift Rate (sermons showing theological shift):")
    print(f"  Current:  {snapshot['drift_rate']['current']:.1f}%")
    print(f"  Previous: {snapshot['drift_rate']['previous']:.1f}%")
    print(f"  Delta:    {snapshot['drift_rate']['delta']:+.1f}%")
    print()

    print("=" * 80)


if __name__ == "__main__":
    import sys

    days = 30
    if len(sys.argv) > 1:
        try:
            days = int(sys.argv[1])
        except ValueError:
            print(f"Invalid days argument: {sys.argv[1]}", file=sys.stderr)
            sys.exit(1)

    print_climate_snapshot(days=days)
