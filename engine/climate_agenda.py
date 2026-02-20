#!/usr/bin/env python3
"""
engine/climate_agenda.py - Climate-First Assembly Agenda

Outputs JSON with:
- climate_snapshot: current vs previous period comparison
- theme_convergence: convergent theological themes
- scripture_focus: dominant scripture books/passages
- observations: Elias-style claims with quote_bank receipts (NEW)
- resonant_sermons: examples with evidence excerpts
- outliers: max 3 anomalies (optional)
"""

import argparse
import json
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from .climate_snapshot import generate_climate_snapshot
from .config import DATABASE_PATH
from .quote_bank import get_quotes


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


def fetch_period_sermons(conn: sqlite3.Connection, start_date: str, end_date: str) -> List[Dict]:
    """Fetch message details for a date range."""
    q = """
    SELECT
        br.video_id,
        br.theological_density,
        br.grace_vs_effort,
        br.hope_vs_fear,
        br.doctrine_vs_experience,
        br.scripture_vs_story,
        br.raw_scores_json,
        v.channel_id,
        c.channel_name,
        v.title,
        v.published_at
    FROM brain_results br
    JOIN videos v ON br.video_id = v.video_id
    JOIN channels c ON v.channel_id = c.channel_id
    WHERE v.published_at >= ? AND v.published_at < ?
    ORDER BY v.published_at DESC
    """

    rows = conn.execute(q, (start_date, end_date)).fetchall()

    items = []
    for r in rows:
        raw = _safe_json_load(r['raw_scores_json'], {})
        zscores = raw.get('zscores', {}).get('axes', {})
        drift_level = raw.get('drift_level', 'unknown')

        drift_mag = 0.0
        if zscores:
            drift_mag = max(abs(z) for z in zscores.values())

        items.append({
            'video_id': r['video_id'],
            'channel_id': r['channel_id'],
            'channel_name': r['channel_name'],
            'title': r['title'],
            'published_at': r['published_at'],
            'theological_density': float(r['theological_density'] or 0.0),
            'axis_scores': {
                'grace_vs_effort': float(r['grace_vs_effort'] or 0.0),
                'hope_vs_fear': float(r['hope_vs_fear'] or 0.0),
                'doctrine_vs_experience': float(r['doctrine_vs_experience'] or 0.0),
                'scripture_vs_story': float(r['scripture_vs_story'] or 0.0),
            },
            'zscores': zscores,
            'drift_level': drift_level,
            'drift_magnitude': drift_mag,
            'category_density': raw.get('category_density', {}),
            'scripture_refs': raw.get('scripture_refs', {}),
        })

    return items


def identify_theme_convergence(items: List[Dict], top_n: int = 3) -> List[Dict]:
    """Identify convergent theological themes from top categories."""
    category_totals = {}
    category_sermons = {}

    for item in items:
        for cat, val in item['category_density'].items():
            category_totals[cat] = category_totals.get(cat, 0.0) + float(val)
            category_sermons.setdefault(cat, []).append(item['video_id'])

    top_categories = sorted(category_totals.items(), key=lambda x: x[1], reverse=True)[:top_n]

    themes = []
    for cat, total_density in top_categories:
        sermon_count = len(category_sermons.get(cat, []))
        themes.append({
            'theme': cat,
            'total_density': round(total_density, 1),
            'sermon_count': sermon_count,
            'avg_density': round(total_density / max(sermon_count, 1), 2),
        })

    return themes


def identify_scripture_focus(items: List[Dict], top_n: int = 5) -> List[Dict]:
    """Identify dominant scripture books."""
    book_totals = {}
    book_sermons = {}

    for item in items:
        for book, count in item['scripture_refs'].items():
            book_totals[book] = book_totals.get(book, 0) + count
            book_sermons.setdefault(book, []).append(item['video_id'])

    top_books = sorted(book_totals.items(), key=lambda x: x[1], reverse=True)[:top_n]

    scripture = []
    for book, total_refs in top_books:
        sermon_count = len(set(book_sermons.get(book, [])))
        scripture.append({
            'book': book,
            'total_references': total_refs,
            'sermon_count': sermon_count,
            'avg_refs_per_sermon': round(total_refs / max(sermon_count, 1), 1),
        })

    return scripture


def _elias_observations(conn: sqlite3.Connection, snapshot: Dict, themes: List[Dict], scripture: List[Dict], days: int) -> List[Dict]:
    """
    Produce a small set of Elias-style observations + quote receipts.
    Not "data analyst voice"—just distilled patterns + 2-3 receipts each.
    """
    obs: List[Dict] = []

    curr = snapshot.get("climate_snapshot", {}).get("current", {})
    avg_axes = curr.get("avg_axes", {}) or {}
    top_books = curr.get("top_books", []) or []
    top_categories = curr.get("top_categories", []) or []

    # Observation 1: Hope tone (from axis)
    hope = float(avg_axes.get("hope_vs_fear", 0.0))
    if hope >= 0.35:
        statement = "Preaching leaned strongly hopeful this month."
        receipts = get_quotes(conn, days=days, category="hope", n=3, distinct_channels=True)
        obs.append({"speaker": "Elias", "statement": statement, "quote_bank": receipts})

    # Observation 2: John dominance (from scripture_focus)
    if scripture:
        # Use the computed focus list (already limited)
        top = scripture[0]
        if len(scripture) >= 2 and top["total_references"] >= 1.4 * scripture[1]["total_references"]:
            statement = "The Gospel of John was the most cited book by a wide margin."
        else:
            statement = f"{top['book'].title()} was the most cited book across messages this month."
        # receipts: scripture_reference evidence is the closest “receipt” category you already have
        receipts = get_quotes(conn, days=days, category="scripture_reference", n=3, distinct_channels=True)
        obs.append({"speaker": "Elias", "statement": statement, "quote_bank": receipts})

    # Observation 3: Story + experience (from theme convergence)
    theme_names = [t["theme"] for t in themes]
    if "story" in theme_names or "experience" in theme_names:
        statement = "Many messages entered through story and lived application before moving into Scripture."
        # Mix: 2 story + 1 experience if possible
        receipts = []
        receipts.extend(get_quotes(conn, days=days, category="story", n=2, distinct_channels=True))
        receipts.extend(get_quotes(conn, days=days, category="experience", n=1, distinct_channels=True))
        obs.append({"speaker": "Elias", "statement": statement, "quote_bank": receipts})

    # Observation 4: Steadier tone (from drift_rate delta)
    drift_rate = snapshot.get("climate_snapshot", {}).get("drift_rate", {}) or {}
    delta = float(drift_rate.get("delta", 0.0))
    # Only mention if meaningful
    if abs(delta) >= 7.5:
        if delta < 0:
            statement = "The overall tone felt steadier than last month—fewer sharp pivots across the network."
        else:
            statement = "The network showed more directional movement than last month—more messages shifting emphasis."
        receipts = []  # optional: you can attach receipts later; volatility is often meta
        obs.append({"speaker": "Elias", "statement": statement, "quote_bank": receipts})

    return obs[:5]


def select_resonant_sermons(
    conn: sqlite3.Connection,
    items: List[Dict],
    themes: List[Dict],
    scripture: List[Dict],
    limit_each: int = 2,
    days: int = 30,
) -> List[Dict]:
    """Pick example messages for top themes with quote_bank receipts."""
    resonant = []
    theme_names = [t['theme'] for t in themes[:3]]

    for theme_name in theme_names:
        candidates = [item for item in items if theme_name in item['category_density']]
        candidates.sort(key=lambda x: x['category_density'].get(theme_name, 0.0), reverse=True)

        for item in candidates[:limit_each]:
            receipts = get_quotes(conn, days=days, category=theme_name, n=3, distinct_channels=False)
            resonant.append({
                'video_id': item['video_id'],
                'channel_name': item['channel_name'],
                'title': item['title'],
                'published_at': item['published_at'],
                'reason': f"High '{theme_name}' density",
                'theological_density': item['theological_density'],
                'category_density': item['category_density'].get(theme_name, 0.0),
                'quote_bank': receipts,
            })

    return resonant


def select_outliers(items: List[Dict], max_outliers: int = 3) -> List[Dict]:
    """Select up to 3 anomaly/strong_shift messages (optional)."""
    anomalies = [item for item in items if item['drift_level'] in ['anomaly', 'strong_shift']]
    anomalies.sort(key=lambda x: x['drift_magnitude'], reverse=True)

    outliers = []
    for item in anomalies[:max_outliers]:
        outliers.append({
            'video_id': item['video_id'],
            'channel_name': item['channel_name'],
            'title': item['title'],
            'published_at': item['published_at'],
            'drift_level': item['drift_level'],
            'drift_magnitude': round(item['drift_magnitude'], 2),
            'axis_scores': item['axis_scores'],
        })

    return outliers


def generate_climate_agenda(days: int = 30, limit: int = 120, limit_each: int = 2) -> Dict:
    """Generate climate-first agenda."""
    conn = _connect()

    snapshot = generate_climate_snapshot(days=days)

    now = datetime.utcnow()
    current_end = now.strftime('%Y-%m-%d %H:%M:%S')
    current_start = (now - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')

    items = fetch_period_sermons(conn, current_start, current_end)

    if limit and len(items) > limit:
        items = items[:limit]

    themes = identify_theme_convergence(items, top_n=3)
    scripture = identify_scripture_focus(items, top_n=5)

    observations = _elias_observations(conn, snapshot, themes, scripture, days=days)
    resonant = select_resonant_sermons(conn, items, themes, scripture, limit_each=limit_each, days=days)
    outliers = select_outliers(items, max_outliers=3)

    conn.close()

    return {
        'climate_snapshot': snapshot,
        'theme_convergence': themes,
        'scripture_focus': scripture,
        'observations': observations,
        'resonant_sermons': resonant,
        'outliers': outliers,
        'metadata': {
            'days': days,
            'limit': limit,
            'total_sermons': len(items),
            'generated_at': datetime.utcnow().isoformat() + 'Z',
        }
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate climate-first assembly agenda")
    parser.add_argument("--days", type=int, default=30, help="Days to look back (default: 30)")
    parser.add_argument("--limit", type=int, default=120, help="Max messages to analyze (default: 120)")
    parser.add_argument("--each", type=int, default=2, help="Resonant messages per theme (default: 2)")
    args = parser.parse_args()

    agenda = generate_climate_agenda(days=args.days, limit=args.limit, limit_each=args.each)
    print(json.dumps(agenda, indent=2))
