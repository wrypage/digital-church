#!/usr/bin/env python3
"""
engine/brain_experiment.py

Experimental comparison of Brain scoring across text variants:
- Summary only
- Full transcript only
- Claims only
- Summary + Claims

Outputs markdown comparison tables to out/experiments/
No database writes. Read-only analysis.

Usage:
  python -m engine.brain_experiment --db db/digital_pulpit.db
"""

import argparse
import json
import os
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from engine.brain import (
    load_brain_config,
    score_categories,
    score_axes,
    theological_density,
    extract_intent_vectors,
    extract_scripture_refs,
)


# ---------------------------
# Database helpers
# ---------------------------

def _connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=5000;")
    return con


def _select_5_sermons(con: sqlite3.Connection) -> List[Dict[str, Any]]:
    """
    Select 5 sermons based on criteria.
    Returns list of dicts with all needed fields.
    """
    sermons = []

    # Sermon 1: Controlled ground truth
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
        WHERE v.video_id = '76d5c3d18fa8cd0b'
        LIMIT 1
    """).fetchone()

    if row:
        sermons.append({
            "label": "Ground Truth (Cana/Selah)",
            "row": row
        })
    else:
        print("Warning: Ground truth sermon not found")

    # Sermon 2: Grace-heavy
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
        JOIN brain_results br ON v.video_id = br.video_id
        WHERE br.grace_vs_effort > 0.5
          AND t.full_text IS NOT NULL
          AND LENGTH(t.full_text) > 0
        ORDER BY br.grace_vs_effort DESC
        LIMIT 1
    """).fetchone()

    if row:
        sermons.append({
            "label": "Grace-Heavy",
            "row": row
        })
    else:
        # Fallback
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
            JOIN brain_results br ON v.video_id = br.video_id
            WHERE t.full_text IS NOT NULL
              AND LENGTH(t.full_text) > 0
            ORDER BY br.grace_vs_effort DESC
            LIMIT 1
        """).fetchone()
        if row:
            sermons.append({
                "label": "Grace-Heavy (fallback)",
                "row": row
            })

    # Sermon 3: Effort-heavy
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
        JOIN brain_results br ON v.video_id = br.video_id
        WHERE br.grace_vs_effort < -0.5
          AND t.full_text IS NOT NULL
          AND LENGTH(t.full_text) > 0
        ORDER BY br.grace_vs_effort ASC
        LIMIT 1
    """).fetchone()

    if row:
        sermons.append({
            "label": "Effort-Heavy",
            "row": row
        })
    else:
        # Fallback
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
            JOIN brain_results br ON v.video_id = br.video_id
            WHERE t.full_text IS NOT NULL
              AND LENGTH(t.full_text) > 0
            ORDER BY br.grace_vs_effort ASC
            LIMIT 1
        """).fetchone()
        if row:
            sermons.append({
                "label": "Effort-Heavy (fallback)",
                "row": row
            })

    # Sermon 4: Doctrinal
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
        JOIN brain_results br ON v.video_id = br.video_id
        WHERE br.doctrine_vs_experience > 0.3
          AND t.full_text IS NOT NULL
          AND LENGTH(t.full_text) > 0
        ORDER BY br.doctrine_vs_experience DESC
        LIMIT 1
    """).fetchone()

    if row:
        sermons.append({
            "label": "Doctrinal",
            "row": row
        })
    else:
        # Fallback
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
            JOIN brain_results br ON v.video_id = br.video_id
            WHERE t.full_text IS NOT NULL
              AND LENGTH(t.full_text) > 0
            ORDER BY br.doctrine_vs_experience DESC
            LIMIT 1
        """).fetchone()
        if row:
            sermons.append({
                "label": "Doctrinal (fallback)",
                "row": row
            })

    # Sermon 5: Highest theological density (or any sermon not yet selected)
    already_selected = [s["row"]["video_id"] for s in sermons if "row" in s]

    placeholders = ",".join("?" * len(already_selected)) if already_selected else "''"

    if already_selected:
        query = f"""
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
            JOIN brain_results br ON v.video_id = br.video_id
            WHERE t.full_text IS NOT NULL
              AND LENGTH(t.full_text) > 0
              AND v.video_id NOT IN ({placeholders})
            ORDER BY br.theological_density DESC
            LIMIT 1
        """
        row = con.execute(query, already_selected).fetchone()
    else:
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
            JOIN brain_results br ON v.video_id = br.video_id
            WHERE t.full_text IS NOT NULL
              AND LENGTH(t.full_text) > 0
            ORDER BY br.theological_density DESC
            LIMIT 1
        """).fetchone()

    if row:
        sermons.append({
            "label": "Highest Theo Density",
            "row": row
        })
    else:
        # Last resort fallback
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
            JOIN brain_results br ON v.video_id = br.video_id
            WHERE t.full_text IS NOT NULL
              AND LENGTH(t.full_text) > 0
            ORDER BY RANDOM()
            LIMIT 1
        """).fetchone()
        if row:
            sermons.append({
                "label": "Random Selection",
                "row": row
            })

    return sermons


# ---------------------------
# Text variant construction
# ---------------------------

def _flatten_claims(claims_json_str: Optional[str]) -> Optional[str]:
    """
    Parse claims_json and return plain text, one claim per line.
    Handle various formats gracefully.
    """
    if not claims_json_str:
        return None

    try:
        claims = json.loads(claims_json_str)
    except Exception:
        return None

    if not claims:
        return None

    lines = []
    if isinstance(claims, list):
        for item in claims:
            if isinstance(item, str):
                lines.append(item)
            elif isinstance(item, dict):
                # Try common keys
                for key in ["claim", "text", "statement", "summary"]:
                    if key in item and item[key]:
                        lines.append(str(item[key]))
                        break

    if not lines:
        return None

    return "\n".join(lines)


def _build_variants(row: sqlite3.Row) -> Dict[str, Any]:
    """
    Build 4 text variants from a sermon row.
    Returns dict with variant texts and metadata.
    """
    full_text = row["full_text"] or ""
    summary_text = row["summary_text"] or ""
    claims_json_str = row["claims_json"]

    claims_flat = _flatten_claims(claims_json_str)

    variants = {}

    # V1: Summary only
    variants["v1"] = {
        "name": "V1 Summary",
        "text": summary_text,
        "skipped": False
    }

    # V2: Full transcript
    variants["v2"] = {
        "name": "V2 Transcript",
        "text": full_text,
        "skipped": False
    }

    # V3: Claims only
    if claims_flat:
        variants["v3"] = {
            "name": "V3 Claims",
            "text": claims_flat,
            "skipped": False
        }
    else:
        variants["v3"] = {
            "name": "V3 Claims",
            "text": "",
            "skipped": True
        }

    # V4: Summary + Claims
    if claims_flat:
        variants["v4"] = {
            "name": "V4 Summary+Claims",
            "text": summary_text + "\n\n" + claims_flat,
            "skipped": False
        }
    else:
        variants["v4"] = {
            "name": "V4 Summary+Claims",
            "text": "",
            "skipped": True
        }

    return variants


# ---------------------------
# Brain scoring
# ---------------------------

def _score_variant(text: str, cfg: Any) -> Dict[str, Any]:
    """
    Run brain scoring on a text variant.
    Returns dict with all computed metrics.
    """
    if not text or not text.strip():
        return {
            "word_count": 0,
            "category_counts": {},
            "category_density": {},
            "theo_density": 0.0,
            "gve": 0.0,
            "hvf": 0.0,
            "dve": 0.0,
            "svs": 0.0,
            "primary_burden": ""
        }

    word_count = len(text.split())

    cat_counts, cat_density = score_categories(text, cfg)
    axis_scores = score_axes(cat_density, cfg)
    theo_dens = theological_density(cat_density)

    # Extract primary burden summary only
    intent = extract_intent_vectors(text)
    primary_burden = ""
    if intent and "primary_burden" in intent:
        pb = intent["primary_burden"]
        if isinstance(pb, dict):
            primary_burden = pb.get("summary", "")

    return {
        "word_count": word_count,
        "category_counts": cat_counts,
        "category_density": cat_density,
        "theo_density": theo_dens,
        "gve": axis_scores.get("grace_vs_effort", 0.0),
        "hvf": axis_scores.get("hope_vs_fear", 0.0),
        "dve": axis_scores.get("doctrine_vs_experience", 0.0),
        "svs": axis_scores.get("scripture_vs_story", 0.0),
        "primary_burden": primary_burden
    }


# ---------------------------
# Output formatting
# ---------------------------

def _format_axis_table(sermon_data: Dict[str, Any]) -> str:
    """
    Format TABLE 1 — AXIS COMPARISON
    """
    lines = []
    lines.append("### TABLE 1 — AXIS COMPARISON")
    lines.append("")

    # Header
    header = "| Variant | word_count | theo_density | grace_vs_effort | hope_vs_fear | doc_vs_exp | scr_vs_story | primary_burden_summary |"
    lines.append(header)
    lines.append("|---------|------------|--------------|-----------------|--------------|------------|--------------|------------------------|")

    # DB baseline row
    db = sermon_data["db"]
    db_burden = ""
    if db.get("raw_scores_json"):
        try:
            raw = json.loads(db["raw_scores_json"])
            intent = raw.get("intent_vectors", {})
            if intent and "primary_burden" in intent:
                pb = intent["primary_burden"]
                if isinstance(pb, dict):
                    db_burden = pb.get("summary", "")[:60]
        except Exception:
            pass

    # Get word count from raw_scores_json or estimate
    db_wc = 0
    if db.get("raw_scores_json"):
        try:
            raw = json.loads(db["raw_scores_json"])
            db_wc = raw.get("word_count", 0)
        except Exception:
            pass

    db_row = f"| DB (current) | {db_wc} | {db.get('db_density', 0.0):.3f} | {db.get('db_gve', 0.0):.3f} | {db.get('db_hvf', 0.0):.3f} | {db.get('db_dve', 0.0):.3f} | {db.get('db_svs', 0.0):.3f} | {db_burden} |"
    lines.append(db_row)

    # Variant rows
    for vkey in ["v1", "v2", "v3", "v4"]:
        vdata = sermon_data["variants"][vkey]
        if vdata["skipped"]:
            row = f"| {vdata['name']} | SKIPPED | SKIPPED | SKIPPED | SKIPPED | SKIPPED | SKIPPED | SKIPPED |"
        else:
            scores = vdata["scores"]
            burden = scores["primary_burden"][:60] if scores["primary_burden"] else ""
            row = f"| {vdata['name']} | {scores['word_count']} | {scores['theo_density']:.3f} | {scores['gve']:.3f} | {scores['hvf']:.3f} | {scores['dve']:.3f} | {scores['svs']:.3f} | {burden} |"
        lines.append(row)

    lines.append("")
    return "\n".join(lines)


def _format_category_table(sermon_data: Dict[str, Any], cfg: Any) -> str:
    """
    Format TABLE 2 — CATEGORY BREAKDOWN
    """
    lines = []
    lines.append("### TABLE 2 — CATEGORY BREAKDOWN (Raw Counts)")
    lines.append("")

    # Header
    header = "| Category | DB | V1 | V2 | V3 | V4 |"
    lines.append(header)
    lines.append("|----------|----|----|----|----|-------|")

    # Get DB counts from raw_scores_json
    db_counts = {}
    db = sermon_data["db"]
    if db.get("raw_scores_json"):
        try:
            raw = json.loads(db["raw_scores_json"])
            db_counts = raw.get("category_counts", {})
        except Exception:
            pass

    # Get all categories from config
    categories = list(cfg.categories.keys())

    for cat in categories:
        db_val = db_counts.get(cat, 0)
        v1_val = sermon_data["variants"]["v1"]["scores"]["category_counts"].get(cat, 0) if not sermon_data["variants"]["v1"]["skipped"] else "-"
        v2_val = sermon_data["variants"]["v2"]["scores"]["category_counts"].get(cat, 0) if not sermon_data["variants"]["v2"]["skipped"] else "-"
        v3_val = sermon_data["variants"]["v3"]["scores"]["category_counts"].get(cat, 0) if not sermon_data["variants"]["v3"]["skipped"] else "-"
        v4_val = sermon_data["variants"]["v4"]["scores"]["category_counts"].get(cat, 0) if not sermon_data["variants"]["v4"]["skipped"] else "-"

        row = f"| {cat} | {db_val} | {v1_val} | {v2_val} | {v3_val} | {v4_val} |"
        lines.append(row)

    lines.append("")

    # Check for scripture_reference inflation
    v1_scr = sermon_data["variants"]["v1"]["scores"]["category_counts"].get("scripture_reference", 0)
    v2_scr = sermon_data["variants"]["v2"]["scores"]["category_counts"].get("scripture_reference", 0)

    if v1_scr > 0 and v2_scr > 2 * v1_scr:
        lines.append(f"⚠ **scripture_reference inflation detected**: V2={v2_scr} vs V1={v1_scr}")
        lines.append("")

    return "\n".join(lines)


def _detect_axis_flips(all_sermons: List[Dict[str, Any]]) -> List[str]:
    """
    Detect axis sign flips vs DB baseline.
    Returns list of formatted strings.
    """
    flips = []

    for sermon in all_sermons:
        title = sermon["title"]
        db = sermon["db"]

        db_gve = db.get("db_gve", 0.0)
        db_hvf = db.get("db_hvf", 0.0)
        db_dve = db.get("db_dve", 0.0)
        db_svs = db.get("db_svs", 0.0)

        for vkey in ["v1", "v2", "v3", "v4"]:
            vdata = sermon["variants"][vkey]
            if vdata["skipped"]:
                continue

            scores = vdata["scores"]
            vname = vdata["name"]

            # Check each axis for sign flip
            if (db_gve > 0) != (scores["gve"] > 0) and abs(scores["gve"]) > 0.05:
                flips.append(f"{title} | grace_vs_effort | DB={db_gve:.3f} | {vname}={scores['gve']:.3f}")

            if (db_hvf > 0) != (scores["hvf"] > 0) and abs(scores["hvf"]) > 0.05:
                flips.append(f"{title} | hope_vs_fear | DB={db_hvf:.3f} | {vname}={scores['hvf']:.3f}")

            if (db_dve > 0) != (scores["dve"] > 0) and abs(scores["dve"]) > 0.05:
                flips.append(f"{title} | doctrine_vs_experience | DB={db_dve:.3f} | {vname}={scores['dve']:.3f}")

            if (db_svs > 0) != (scores["svs"] > 0) and abs(scores["svs"]) > 0.05:
                flips.append(f"{title} | scripture_vs_story | DB={db_svs:.3f} | {vname}={scores['svs']:.3f}")

    return flips


def _calculate_density_ranges(all_sermons: List[Dict[str, Any]]) -> List[str]:
    """
    Calculate density ranges for each sermon.
    Returns list of formatted strings.
    """
    ranges = []

    for sermon in all_sermons:
        title = sermon["title"]
        densities = []

        for vkey in ["v1", "v2", "v3", "v4"]:
            vdata = sermon["variants"][vkey]
            if not vdata["skipped"]:
                densities.append(vdata["scores"]["theo_density"])

        if densities:
            min_d = min(densities)
            max_d = max(densities)
            ratio = max_d / min_d if min_d > 0 else float('inf')
            ranges.append(f"{title} | min={min_d:.3f} | max={max_d:.3f} | ratio={ratio:.2f}x")

    return ranges


# ---------------------------
# Main experiment
# ---------------------------

def run_experiment(db_path: str) -> Dict[str, Any]:
    """
    Run the full experiment.
    Returns dict with output file path and summary stats.
    """
    # Load config
    cfg = load_brain_config()

    # Connect to DB
    con = _connect(db_path)

    try:
        # Select 5 sermons
        selected = _select_5_sermons(con)

        if not selected:
            raise RuntimeError("No sermons found")

        # Process each sermon
        all_sermons = []

        for item in selected:
            label = item["label"]
            row = item["row"]

            print(f"Processing: {label} - {row['title']}")

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
                "label": label,
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

            all_sermons.append(sermon_data)

        # Generate output
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        output_dir = "out/experiments"
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, f"brain_variants_{timestamp}.md")

        with open(output_file, "w", encoding="utf-8") as f:
            f.write("# Brain Variant Experiment\n\n")
            f.write(f"**Generated:** {datetime.utcnow().isoformat()}Z\n\n")
            f.write("**Objective:** Compare Brain scoring across text variants (summary, transcript, claims, summary+claims)\n\n")
            f.write("---\n\n")

            # Write sermon tables
            for sermon in all_sermons:
                f.write(f"## {sermon['label']}\n\n")
                f.write(f"**Title:** {sermon['title']}\n\n")
                f.write(f"**Channel:** {sermon['channel']}\n\n")
                f.write(f"**Video ID:** {sermon['video_id']}\n\n")
                f.write("---\n\n")

                f.write(_format_axis_table(sermon))
                f.write("\n")
                f.write(_format_category_table(sermon, cfg))
                f.write("\n")
                f.write("---\n\n")

            # Summary section
            f.write("# SUMMARY\n\n")

            f.write("## AXIS FLIPS vs DB BASELINE\n\n")
            flips = _detect_axis_flips(all_sermons)
            if flips:
                for flip in flips:
                    f.write(f"- {flip}\n")
            else:
                f.write("No axis flips detected.\n")
            f.write("\n")

            f.write("## DENSITY RANGE\n\n")
            ranges = _calculate_density_ranges(all_sermons)
            for rng in ranges:
                f.write(f"- {rng}\n")
            f.write("\n")

        # Return summary
        return {
            "experiment_file": output_file,
            "sermons_tested": [s["title"] for s in all_sermons],
            "axis_flips_found": len(flips)
        }

    finally:
        con.close()


# ---------------------------
# CLI
# ---------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Brain variant experiment: compare scoring across text variants"
    )
    parser.add_argument(
        "--db",
        default="db/digital_pulpit.db",
        help="Path to database file"
    )

    args = parser.parse_args()

    result = run_experiment(args.db)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
