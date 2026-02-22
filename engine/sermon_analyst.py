#!/usr/bin/env python3
"""
engine/sermon_analyst.py

Bottom-up Sermon Analyst layer (GPT-4.1):
- Pulls transcript for each video
- Produces structured semantic analysis (themes, claims, receipts)
- Adds 4 triads (1,2,4,5) with normalized weights
- Stores results in sermon_analysis (skip if already analyzed unless --force)

Testing controls:
  --dry_run
  --limit N
  --video_id XYZ
  --days N
  --force
  --max_cost_usd
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

# ----------------------------
# Config
# ----------------------------

DATABASE_PATH = os.environ.get("DP_DB_PATH", "db/digital_pulpit.db")


def _get_openai_client():
    # Uses the modern OpenAI python client
    from openai import OpenAI

    # Try to get API key from multiple sources
    api_key = os.environ.get("OPENAI_API_KEY")

    # If not in environment, try loading from .env file
    if not api_key:
        env_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
        if os.path.exists(env_file):
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("OPENAI_API_KEY="):
                        api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
                        break

    if not api_key:
        raise ValueError(
            "OpenAI API key not found. Please either:\n"
            "  1. Set OPENAI_API_KEY environment variable, or\n"
            "  2. Create a .env file with: OPENAI_API_KEY=your-key-here"
        )

    return OpenAI(api_key=api_key)


# ----------------------------
# GPT call helper (RESILIENT JSON)
# ----------------------------

def _call_gpt41_json(system: str, user: str, max_output_tokens: int = 2000) -> Dict[str, Any]:
    """
    Returns parsed JSON object.

    We request response_format json_object, but in rare cases you can still get invalid JSON
    (most commonly due to truncation at the token limit).
    Strategy:
      1) Call GPT with json_object + requested token limit.
      2) If JSON parsing fails, retry once with a larger token limit and an added brevity constraint.
      3) If it still fails, run a small "JSON repair" call that returns only fixed JSON.
    """
    client = _get_openai_client()

    def _chat_call(sys_msg: str, user_msg: str, out_tokens: int) -> str:
        resp = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": sys_msg},
                {"role": "user", "content": user_msg},
            ],
            response_format={"type": "json_object"},
            max_tokens=out_tokens,
        )
        return (resp.choices[0].message.content or "")

    def _call_any(sys_msg: str, user_msg: str, out_tokens: int) -> str:
        return _chat_call(sys_msg, user_msg, out_tokens)

    def _loads(s: str) -> Dict[str, Any]:
        s = (s or "").strip()
        return json.loads(s) if s else {}

    # 1) Primary attempt
    try:
        text = _call_any(system, user, max_output_tokens)
        return _loads(text)
    except json.JSONDecodeError:
        pass
    except Exception as e:
        raise RuntimeError(f"OpenAI call failed: {e}") from e

    # 2) Retry with more room + stricter brevity constraints
    retry_user = user + "\n\nBrevity constraints (to avoid truncation):\n- Keep EVERY string under 220 characters.\n- Receipts: excerpt <= 25 words.\n- Triad reasons: each bullet <= 12 words.\n- Return ONLY JSON.\n"
    try:
        text = _call_any(system, retry_user, max(max_output_tokens, 2600))
        return _loads(text)
    except json.JSONDecodeError:
        bad_text = (text or "")[:12000]  # cap what we send to repair
    except Exception as e:
        raise RuntimeError(f"OpenAI call failed on retry: {e}") from e

    # 3) JSON repair (cheap, short)
    repair_system = "You are a strict JSON repair tool. Return ONLY valid JSON, no markdown."
    repair_user = "Fix the following so it becomes valid JSON matching the expected schema.\nDo NOT add commentary. Return ONLY JSON.\n\n" + bad_text
    try:
        fixed = _call_any(repair_system, repair_user, 1200)
        return _loads(fixed)
    except Exception as e:
        raise RuntimeError(
            f"OpenAI call failed: could not produce valid JSON after repair attempt: {e}"
        ) from e


# ----------------------------
# DB helpers
# ----------------------------

def _connect(db_path: str = DATABASE_PATH) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=5000;")
    return con


def _ensure_sermon_analysis_table(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS sermon_analysis (
            analysis_id INTEGER PRIMARY KEY AUTOINCREMENT,
            video_id TEXT NOT NULL UNIQUE,
            analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            title TEXT,
            channel_name TEXT,
            published_at TEXT,
            analysis_json TEXT,
            themes_json TEXT,
            claims_json TEXT,
            receipts_json TEXT,
            triads_json TEXT,
            tone_json TEXT,
            pastoral_burden TEXT,
            cost_usd REAL DEFAULT 0.0
        );
        """
    )
    con.commit()


def _already_analyzed(con: sqlite3.Connection, video_id: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sermon_analysis WHERE video_id = ? LIMIT 1;",
        (video_id,),
    ).fetchone()
    return row is not None


# ----------------------------
# Prompt
# ----------------------------

SYSTEM_PROMPT = """You are a theological sermon analyst building a bottom-up semantic record for one sermon.
Your output MUST be strict JSON (no markdown, no commentary).

Constraints:
- Do not include housekeeping/announcements as receipts.
- Receipts must be short verbatim excerpts (1–3 sentences), readable, theologically meaningful, and <= 25 words.
- Themes must be semantic (plain-English theological ideas), NOT labels like "scripture_reference", "story", "experience".
- Claims must be propositional (short statements that could be affirmed/denied).
- Triads must have normalized weights that sum to 1.0.

Return keys EXACTLY as specified.
"""

USER_TEMPLATE = """Analyze this ONE sermon transcript.

Metadata:
- Title: {title}
- Channel: {channel_name}
- Published: {published_at}

Transcript (verbatim, may include noise):
{transcript}

Return JSON with this shape:

{{
  "core_thesis": "1–2 sentences",
  "semantic_themes": ["5–8 themes, plain-English, sermon-specific"],
  "key_claims": ["3–6 short theological claims"],
  "pastoral_burden": "What the preacher is trying to produce in the hearer (1–2 sentences)",
  "tone": {{
    "primary": "one of: exhortational | comforting | warning | instructional | celebratory | lament",
    "notes": "1 sentence"
  }},
  "receipts": [
    {{
      "excerpt": "verbatim quote 1–3 sentences",
      "supports": "thesis | claim",
      "notes": "why this matters (short)"
    }}
  ],
  "triads": {{
    "authority_experience_formation": {{
      "weights": {{"authority": 0.0, "experience": 0.0, "formation": 0.0}},
      "reasons": ["2–3 bullets citing evidence cues (not long quotes)"]
    }},
    "exposition_application_imagination": {{
      "weights": {{"exposition": 0.0, "application": 0.0, "imagination": 0.0}},
      "reasons": ["2–3 bullets"]
    }},
    "stability_momentum_fragility": {{
      "weights": {{"stability": 0.0, "momentum": 0.0, "fragility": 0.0}},
      "reasons": ["2–3 bullets"]
    }},
    "christ_church_culture": {{
      "weights": {{"christ": 0.0, "church": 0.0, "culture": 0.0}},
      "reasons": ["2–3 bullets"]
    }}
  }},
  "quality": {{
    "housekeeping_removed": true,
    "notes": "1 sentence on transcript quality"
  }}
}}

Important:
- Provide 3–5 receipts (not more). Keep each excerpt <= 25 words.
- Avoid copying housekeeping or web addresses.
"""


# ----------------------------
# Cost estimate (simple, conservative)
# ----------------------------

def _estimate_cost_usd(transcript: str, output_tokens: int = 2000) -> float:
    # Conservative ballpark based on your observed ~$0.04–0.05 per sermon.
    # This is just for budgeting guardrails; not exact billing.
    # Using transcript size as a proxy:
    n_chars = len(transcript or "")
    # crude scaling: ~30k chars ~ $0.05
    base = 0.05 * (n_chars / 30000.0)
    # add small constant for overhead
    return max(0.02, min(0.25, base + 0.01))


# ----------------------------
# Candidate selection
# ----------------------------

def _parse_iso(dt_str: str) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
    except Exception:
        return None


def _fetch_candidates(con: sqlite3.Connection, days: int, limit: int, video_id: Optional[str] = None) -> List[sqlite3.Row]:
    if video_id:
        rows = con.execute(
            """
            SELECT v.video_id, v.title, c.channel_name, v.published_at, t.full_text
            FROM videos v
            JOIN transcripts t ON t.video_id = v.video_id
            JOIN channels c ON v.channel_id = c.channel_id
            WHERE v.video_id = ?
            LIMIT 1;
            """,
            (video_id,),
        ).fetchall()
        return rows

    cutoff = datetime.utcnow() - timedelta(days=days)

    rows = con.execute(
        """
        SELECT v.video_id, v.title, c.channel_name, v.published_at, t.full_text
        FROM videos v
        JOIN transcripts t ON t.video_id = v.video_id
        JOIN channels c ON v.channel_id = c.channel_id
        LEFT JOIN sermon_analysis s ON s.video_id = v.video_id
        WHERE s.video_id IS NULL
          AND t.full_text IS NOT NULL
          AND LENGTH(TRIM(t.full_text)) > 0
        ORDER BY v.published_at DESC
        LIMIT ?;
        """,
        (limit * 5,),
    ).fetchall()

    out: List[sqlite3.Row] = []
    for r in rows:
        pub = _parse_iso(r["published_at"])
        if pub is None:
            out.append(r)
        else:
            if pub.replace(tzinfo=None) >= cutoff:
                out.append(r)
        if len(out) >= limit:
            break

    return out


# ----------------------------
# Analysis + store
# ----------------------------

def _analyze_one(r: sqlite3.Row) -> Dict[str, Any]:
    title = r["title"] or ""
    channel_name = r["channel_name"] or ""
    published_at = r["published_at"] or ""
    transcript = r["full_text"] or ""

    user = USER_TEMPLATE.format(
        title=title,
        channel_name=channel_name,
        published_at=published_at,
        transcript=transcript,
    )

    # IMPORTANT: increased from 1400 -> 2000 to reduce truncation/JSON corruption
    analysis = _call_gpt41_json(SYSTEM_PROMPT, user, max_output_tokens=2000)
    return analysis


def _store_analysis(con: sqlite3.Connection, r: sqlite3.Row, analysis: Dict[str, Any], cost_usd: float) -> None:
    video_id = r["video_id"]
    title = r["title"] or ""
    channel_name = r["channel_name"] or ""
    published_at = r["published_at"] or ""

    themes = analysis.get("semantic_themes") or []
    claims = analysis.get("key_claims") or []
    receipts = analysis.get("receipts") or []
    triads = analysis.get("triads") or {}
    tone = analysis.get("tone") or {}
    pastoral_burden = analysis.get("pastoral_burden") or ""

    con.execute(
        """
        INSERT OR REPLACE INTO sermon_analysis (
            video_id, title, channel_name, published_at,
            analysis_json, themes_json, claims_json, receipts_json,
            triads_json, tone_json, pastoral_burden, cost_usd
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (
            video_id,
            title,
            channel_name,
            published_at,
            json.dumps(analysis, ensure_ascii=False),
            json.dumps(themes, ensure_ascii=False),
            json.dumps(claims, ensure_ascii=False),
            json.dumps(receipts, ensure_ascii=False),
            json.dumps(triads, ensure_ascii=False),
            json.dumps(tone, ensure_ascii=False),
            pastoral_burden,
            float(cost_usd),
        ),
    )
    con.commit()


# ----------------------------
# CLI
# ----------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DATABASE_PATH)
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--video_id", default=None)
    ap.add_argument("--dry_run", action="store_true")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--max_cost_usd", type=float, default=9999.0)
    args = ap.parse_args()

    con = _connect(args.db)
    try:
        _ensure_sermon_analysis_table(con)

        candidates = _fetch_candidates(con, days=args.days, limit=args.limit, video_id=args.video_id)
        if not candidates:
            print("No candidates found.")
            return

        total_est = 0.0
        queue: List[sqlite3.Row] = []

        for r in candidates:
            vid = r["video_id"]
            if (not args.force) and _already_analyzed(con, vid):
                continue
            est = _estimate_cost_usd(r["full_text"] or "", output_tokens=2000)
            if total_est + est > args.max_cost_usd:
                break
            total_est += est
            queue.append(r)

        if args.dry_run:
            print(f"Dry run. Would analyze {len(queue)} sermons. Estimated cost: ${total_est:.2f}")
            for r in queue:
                print(f"- {r['video_id']} | {r['channel_name']} | {r['title']}")
            return

        print(f"Analyzing {len(queue)} sermons (est cost ${total_est:.2f})...")

        for r in queue:
            vid = r["video_id"]
            title = r["title"] or ""
            print(f"\nAnalyzing {vid} | {title}")
            t0 = time.time()

            analysis = _analyze_one(r)
            cost = _estimate_cost_usd(r["full_text"] or "", output_tokens=2000)
            _store_analysis(con, r, analysis, cost)

            dt = time.time() - t0
            themes = analysis.get("semantic_themes") or []
            if themes:
                print(f"  themes: {'; '.join(themes[:5])}")
            print(f"  stored in {dt:.1f}s")

    finally:
        con.close()


if __name__ == "__main__":
    main()