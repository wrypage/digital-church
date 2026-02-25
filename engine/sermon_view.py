#!/usr/bin/env python3
"""
engine/sermon_view.py

Human-readable renderer for sermon_analysis.

Run:
  python -m engine.sermon_view --video_id VIDEO_ID
"""

import argparse
import json
import sqlite3
from typing import Any, Dict

from engine.config import DATABASE_PATH


def connect():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def load_analysis(conn, video_id: str) -> Dict[str, Any]:
    row = conn.execute("""
        SELECT
            sa.analysis_json,
            v.title,
            v.published_at,
            c.channel_name
        FROM sermon_analysis sa
        JOIN videos v ON v.video_id = sa.video_id
        LEFT JOIN channels c ON c.channel_id = v.channel_id
        WHERE sa.video_id = ?
    """, (video_id,)).fetchone()

    if not row:
        raise ValueError("No analysis found for that video_id.")

    analysis = json.loads(row["analysis_json"])
    analysis["_meta"] = {
        "title": row["title"],
        "published_at": row["published_at"],
        "channel_name": row["channel_name"],
    }
    return analysis


def render(analysis: Dict[str, Any]) -> str:
    meta = analysis["_meta"]

    lines = []

    lines.append(f"# {meta['title']}")
    lines.append(f"{meta['channel_name']} — {meta['published_at']}")
    lines.append("")

    lines.append("## Core Thesis")
    lines.append(analysis.get("core_thesis", ""))
    lines.append("")

    lines.append("## Primary Themes")
    for t in analysis.get("semantic_themes", []):
        lines.append(f"- {t}")
    lines.append("")

    lines.append("## Key Theological Claims")
    for c in analysis.get("key_claims", []):
        lines.append(f"- {c}")
    lines.append("")

    lines.append("## Pastoral Burden")
    lines.append(analysis.get("pastoral_burden", ""))
    lines.append("")

    tone = analysis.get("tone", {})
    lines.append("## Tone")
    lines.append(f"Primary: {tone.get('primary', '')}")
    lines.append(tone.get("notes", ""))
    lines.append("")

    lines.append("## Triads")
    triads = analysis.get("triads", {})
    for name, obj in triads.items():
        lines.append(f"### {name.replace('_', ' ').title()}")
        weights = obj.get("weights", {})
        for k, v in weights.items():
            lines.append(f"- {k.title()}: {round(v * 100)}%")
        lines.append("")
    lines.append("")

    lines.append("## Key Receipts")
    for r in analysis.get("receipts", []):
        lines.append(f"> {r.get('excerpt','')}")
        lines.append("")
    lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--video_id", required=True)
    args = parser.parse_args()

    conn = connect()
    analysis = load_analysis(conn, args.video_id)
    output = render(analysis)

    print(output)


if __name__ == "__main__":
    main()