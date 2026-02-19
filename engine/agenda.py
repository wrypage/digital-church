#!/usr/bin/env python3
"""
engine/agenda.py - Assembly Agenda Generator

Selects analyzed sermons from last N days and categorizes them by:
1) drift (anomaly/strong/moderate) - top 5 by drift magnitude
2) imbalance - top 5 by max absolute axis score
3) stable exemplars - low drift magnitude

Attaches evidence snippets from brain_evidence for each item.
"""

import argparse
import json
import math
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from .config import DATABASE_PATH


def _connect(db_path: str = DATABASE_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _safe_json_load(s: Optional[str], default):
    if not s:
        return default
    try:
        return json.loads(s)
    except Exception:
        return default


def compute_drift_magnitude(zscores: Dict[str, float]) -> float:
    """Compute drift magnitude as max absolute zscore across axes."""
    if not zscores:
        return 0.0
    return max(abs(z) for z in zscores.values())


def compute_imbalance_magnitude(axis_scores: Dict[str, float]) -> float:
    """Compute imbalance magnitude as max absolute axis score."""
    if not axis_scores:
        return 0.0
    return max(abs(s) for s in axis_scores.values())


def fetch_recent_analyzed(conn: sqlite3.Connection, days_back=None, limit=None) -> List[Dict]:
    """
    Fetch analyzed sermons by days or count limit.
    """
    q_base = """
    SELECT
        br.video_id,
        br.theological_density,
        br.grace_vs_effort,
        br.hope_vs_fear,
        br.doctrine_vs_experience,
        br.scripture_vs_story,
        br.top_categories,
        br.raw_scores_json,
        v.channel_id,
        c.channel_name,
        v.title,
        v.published_at
    FROM brain_results br
    JOIN videos v ON br.video_id = v.video_id
    JOIN channels c ON v.channel_id = c.channel_id
    """

    if days_back is not None:
        cutoff = (datetime.utcnow() - timedelta(days=days_back)).strftime('%Y-%m-%d %H:%M:%S')
        q = q_base + "WHERE v.published_at >= ? ORDER BY v.published_at DESC"
        rows = conn.execute(q, (cutoff,)).fetchall()
    else:
        q = q_base + "ORDER BY v.published_at DESC LIMIT ?"
        rows = conn.execute(q, (limit,)).fetchall()

    items = []
    for r in rows:
        raw = _safe_json_load(r['raw_scores_json'], {})
        zscores = raw.get('zscores', {}).get('axes', {})
        axis_scores = raw.get('axis_scores', {})
        drift_level = raw.get('drift_level', 'unknown')

        drift_mag = compute_drift_magnitude(zscores)
        imbalance_mag = compute_imbalance_magnitude(axis_scores)

        items.append({
            'video_id': r['video_id'],
            'channel_id': r['channel_id'],
            'channel_name': r['channel_name'],
            'title': r['title'],
            'published_at': r['published_at'],
            'theological_density': float(r['theological_density'] or 0.0),
            'axis_scores': axis_scores,
            'zscores': zscores,
            'drift_level': drift_level,
            'drift_magnitude': drift_mag,
            'imbalance_magnitude': imbalance_mag,
            'top_categories': _safe_json_load(r['top_categories'], []),
            'raw': raw,
        })

    return items


def fetch_evidence(conn: sqlite3.Connection, video_id: str, limit: int = 3) -> List[Dict]:
    """
    Fetch evidence snippets for a video (axis and category evidence).
    """
    q = """
    SELECT axis, category, keyword, excerpt, start_char
    FROM brain_evidence
    WHERE video_id = ?
    ORDER BY
        CASE WHEN axis IS NOT NULL THEN 0 ELSE 1 END,
        start_char
    LIMIT ?
    """

    rows = conn.execute(q, (video_id, limit)).fetchall()

    evidence = []
    for r in rows:
        evidence.append({
            'axis': r['axis'],
            'category': r['category'],
            'keyword': r['keyword'],
            'excerpt': r['excerpt'],
            'start_char': int(r['start_char'] or 0),
        })

    return evidence


def generate_agenda(days_back=None, limit=120, limit_each=5) -> Dict:
    """
    Generate Assembly agenda from recent analyzed sermons.

    Returns dict with keys:
    - drift: sermons with significant drift (anomaly/strong/moderate)
    - imbalance: sermons with extreme axis imbalances
    - stable: stable exemplars with low drift
    - metadata: summary stats
    """
    conn = _connect()
    items = fetch_recent_analyzed(conn, days_back=days_back, limit=limit)

    if not items:
        return {
            'drift': [],
            'imbalance': [],
            'stable': [],
            'metadata': {
                'days_back': days_back,
                'limit': limit,
                'total_sermons': 0,
                'generated_at': datetime.utcnow().isoformat() + 'Z',
            }
        }

    # Bucket 1: Drift (anomaly/strong/moderate)
    drift_items = [
        item for item in items
        if item['drift_level'] in ['anomaly', 'strong_shift', 'moderate_shift']
    ]
    drift_items.sort(key=lambda x: x['drift_magnitude'], reverse=True)
    drift_top = drift_items[:limit_each]

    # Bucket 2: Imbalance (high axis score regardless of drift)
    imbalance_items = sorted(items, key=lambda x: x['imbalance_magnitude'], reverse=True)
    imbalance_top = imbalance_items[:limit_each]

    # Bucket 3: Stable (low drift, good exemplars)
    stable_items = [
        item for item in items
        if item['drift_level'] in ['stable', 'insufficient_history']
        and item['drift_magnitude'] < 1.0
    ]
    stable_items.sort(key=lambda x: x['theological_density'], reverse=True)
    stable_top = stable_items[:min(3, limit_each)]  # Fewer stable items needed

    # Attach evidence to each item
    def attach_evidence(item):
        evidence = fetch_evidence(conn, item['video_id'], limit=3)
        return {
            'video_id': item['video_id'],
            'channel_id': item['channel_id'],
            'channel_name': item['channel_name'],
            'title': item['title'],
            'published_at': item['published_at'],
            'theological_density': item['theological_density'],
            'drift_level': item['drift_level'],
            'drift_magnitude': item['drift_magnitude'],
            'imbalance_magnitude': item['imbalance_magnitude'],
            'axis_scores': item['axis_scores'],
            'zscores': item['zscores'],
            'top_categories': item['top_categories'],
            'evidence': evidence,
        }

    agenda = {
        'drift': [attach_evidence(item) for item in drift_top],
        'imbalance': [attach_evidence(item) for item in imbalance_top],
        'stable': [attach_evidence(item) for item in stable_top],
        'metadata': {
            'days_back': days_back,
            'limit': limit,
            'total_sermons': len(items),
            'drift_count': len(drift_items),
            'stable_count': len(stable_items),
            'generated_at': datetime.utcnow().isoformat() + 'Z',
        }
    }

    conn.close()
    return agenda


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=None)
    parser.add_argument("--limit", type=int, default=120)
    parser.add_argument("--each", type=int, default=5)
    args = parser.parse_args()

    agenda = generate_agenda(
        days_back=args.days,
        limit=args.limit,
        limit_each=args.each
    )

    print(json.dumps(agenda, indent=2))
