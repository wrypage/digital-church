#!/usr/bin/env python3
"""
engine/quote_bank.py

Upgraded quote intelligence:

- Rhetorical density scoring
- Claim-language detection
- Fragment suppression
- Filler penalty
- Theological keyword boosting
- No time slicing hacks
"""

from __future__ import annotations
import re
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional


# ---------- SIGNAL WORDS ----------

THEOLOGICAL_WORDS = [
    "jesus", "christ", "lord", "gospel", "kingdom",
    "sin", "repent", "repentance", "grace", "mercy",
    "faith", "salvation", "holy spirit", "cross",
    "resurrection", "truth", "holiness", "obedience",
    "righteousness", "justification", "sanctification"
]

RHETORICAL_MARKERS = [
    "therefore", "because", "but", "yet", "however",
    "so that", "if", "then", "not merely", "rather",
    "the problem", "the danger", "the question",
    "what this means", "in other words"
]

TENSION_WORDS = [
    "danger", "drift", "illusion", "deception",
    "fear", "hope", "cost", "surrender",
    "authority", "conviction", "judgment"
]

FILLER_PHRASES = [
    "welcome to", "thanks for joining",
    "let's dive", "turn with me",
    "today we're going", "as you're turning",
    "joining us today"
]

URL_PATTERN = re.compile(r"\b[a-z0-9-]+\.(com|org|net|io|co)\b", re.I)


# ---------- SCORING ----------

def _ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _fragment_penalty(text: str) -> float:
    if not text:
        return 1.0
    if text.strip().startswith(("and ", "but ", "so ")):
        return 0.7
    if len(text.split()) < 12:
        return 1.0
    return 0.0


def _filler_penalty(text: str) -> float:
    low = text.lower()
    if any(p in low for p in FILLER_PHRASES):
        return 1.0
    if URL_PATTERN.search(low):
        return 1.0
    return 0.0


def _theological_score(text: str) -> float:
    low = text.lower()
    return sum(1.2 for w in THEOLOGICAL_WORDS if w in low)


def _rhetorical_score(text: str) -> float:
    low = text.lower()
    score = 0.0
    for m in RHETORICAL_MARKERS:
        if m in low:
            score += 0.8
    return score


def _tension_score(text: str) -> float:
    low = text.lower()
    return sum(0.7 for w in TENSION_WORDS if w in low)


def _claim_shape_score(text: str) -> float:
    # Prefer sentences with structure (subject + verb + object)
    words = text.split()
    if len(words) < 15:
        return 0.0
    if "." in text or ";" in text:
        return 0.5
    return 0.2


def _intelligence_score(text: str) -> float:
    text = _ws(text)
    if not text:
        return 0.0

    score = 0.0
    score += _theological_score(text)
    score += _rhetorical_score(text)
    score += _tension_score(text)
    score += _claim_shape_score(text)

    score -= _fragment_penalty(text)
    score -= _filler_penalty(text)

    return score


# ---------- MAIN ----------

def get_quotes(
    conn: sqlite3.Connection,
    days: int = 30,
    category: Optional[str] = None,
    n: int = 8,
    distinct_channels: bool = True,
    must_contain: Optional[List[str]] = None,
    video_id: Optional[str] = None,
) -> List[Dict[str, Any]]:

    cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")

    where = []
    params: List[Any] = []

    if category:
        where.append("be.category = ?")
        params.append(category)

    if video_id:
        where.append("be.video_id = ?")
        params.append(video_id)

    where.append("v.published_at >= ?")
    params.append(cutoff)

    where_sql = "WHERE " + " AND ".join(where)

    sql = f"""
        SELECT
            be.video_id,
            be.category,
            be.excerpt,
            v.title,
            v.published_at,
            c.channel_name
        FROM brain_evidence be
        JOIN videos v ON be.video_id = v.video_id
        LEFT JOIN channels c ON v.channel_id = c.channel_id
        {where_sql}
        LIMIT 4000
    """

    rows = conn.execute(sql, tuple(params)).fetchall()

    candidates = []

    for r in rows:
        text = r["excerpt"] or ""
        if must_contain:
            low = text.lower()
            if not any(token.lower() in low for token in must_contain):
                continue

        score = _intelligence_score(text)
        if score <= 0:
            continue

        candidates.append((score, r))

    # Sort by intelligence descending
    candidates.sort(key=lambda x: x[0], reverse=True)

    out = []
    used_channels = set()

    for score, r in candidates:
        ch = (r["channel_name"] or "").lower()

        if distinct_channels and ch in used_channels:
            continue

        excerpt = _ws(r["excerpt"])
        if len(excerpt.split()) > 45:
            excerpt = " ".join(excerpt.split()[:45]) + "…"

        out.append({
            "video_id": r["video_id"],
            "channel_name": r["channel_name"],
            "title": r["title"],
            "published_at": r["published_at"],
            "category": r["category"],
            "excerpt": excerpt,
            "intelligence_score": round(score, 2)
        })

        used_channels.add(ch)

        if len(out) >= n:
            break

    return out


def get_quotes_for_video(
    conn: sqlite3.Connection,
    video_id: str,
    category: Optional[str] = None,
    n: int = 3,
    must_contain: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    return get_quotes(
        conn,
        days=3650,
        category=category,
        n=n,
        distinct_channels=False,
        must_contain=must_contain,
        video_id=video_id,
    )