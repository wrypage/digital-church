#!/usr/bin/env python3
"""
run_single_video_experiment.py

Run brain variant experiment on a single video.

Usage:
  python run_single_video_experiment.py --video_id 78db72267e74fa70 --db db/digital_pulpit.db
"""

import argparse
import json
import os
import sqlite3
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

# Import from brain_experiment
sys.path.insert(0, os.path.dirname(__file__))
from engine.brain_experiment import (
    _connect,
    _build_variants,
    _score_variant,
    _format_axis_table,
    _format_category_table,
    load_brain_config,
)


def run_single_experiment(db_path: str, video_id: str) -> Dict[str, Any]:
    """Run experiment on a single video."""

    cfg = load_brain_config()
    con = _connect(db_path)

    try:
        # Get video info
        row = con.execute("""
            SELECT
                v.video_id, v.title,
                c.channel_name,
                t.full_text, t.summary_text,
                sa.claims_json,
                br.theological_density as db_density,
                br.grace_vs_effort as db_gve,
                br.hope_vs_fear as db_hvf,
                br.doctrine_vs_experience as db_dve,
                br.scripture_vs_story as db_svs,
                br.raw_scores_json
            FROM videos v
            JOIN channels c ON v.channel_id = c.channel_id
            JOIN transcripts t ON v.video_id = t.video_id
            LEFT JOIN sermon_analysis sa ON v.video_id = sa.video_id
            LEFT JOIN brain_results br ON v.video_id = br.video_id
            WHERE v.video_id = ?
            LIMIT 1
        """, (video_id,)).fetchone()

        if not row:
            raise RuntimeError(f"Video {video_id} not found")

        print(f"Processing: {row['title']}")

        # Build variants
        variants = _build_variants(row)

        # Score each variant
        for vkey in ["v1", "v2", "v3", "v4"]:
            vdata = variants[vkey]
            if not vdata["skipped"]:
                scores = _score_variant(vdata["text"], cfg)
                vdata["scores"] = scores
            else:
                vdata["scores"] = {}

        sermon_data = {
            "label": "Single Video Test",
            "title": row["title"],
            "channel": row["channel_name"],
            "video_id": row["video_id"],
            "db": {
                "db_density": row["db_density"],
                "db_gve": row["db_gve"],
                "db_hvf": row["db_hvf"],
                "db_dve": row["db_dve"],
                "db_svs": row["db_svs"],
                "raw_scores_json": row["raw_scores_json"]
            },
            "variants": variants
        }

        # Generate output
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        output_dir = "out/experiments"
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f"brain_single_{video_id}_{timestamp}.md")

        with open(output_file, "w", encoding="utf-8") as f:
            f.write("# Brain Variant Experiment - Single Video\n\n")
            f.write(f"**Generated:** {datetime.utcnow().isoformat()}Z\n\n")
            f.write(f"**Video ID:** {video_id}\n\n")
            f.write("---\n\n")

            f.write(f"## {sermon_data['title']}\n\n")
            f.write(f"**Channel:** {sermon_data['channel']}\n\n")
            f.write("---\n\n")

            f.write(_format_axis_table(sermon_data))
            f.write("\n")
            f.write(_format_category_table(sermon_data, cfg))
            f.write("\n")

        return {
            "experiment_file": output_file,
            "video_id": video_id,
            "title": row["title"]
        }

    finally:
        con.close()


def main():
    parser = argparse.ArgumentParser(
        description="Run brain variant experiment on a single video"
    )
    parser.add_argument(
        "--db",
        default="db/digital_pulpit.db",
        help="Path to database file"
    )
    parser.add_argument(
        "--video_id",
        required=True,
        help="Video ID to analyze"
    )

    args = parser.parse_args()

    result = run_single_experiment(args.db, args.video_id)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
