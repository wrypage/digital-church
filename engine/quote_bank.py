#!/usr/bin/env python3
"""
engine/quote_bank.py

Quote selection utilities for Elias + Assembly.
Goals:
- Pull short "receipts" from brain_evidence, joined to videos/channels.
- Filter out low-signal promo phrases (welcome/subscribe/podcast/etc) as a preference.
- Encourage diversity (different channels) when possible.
- De-duplicate near-identical excerpts.
- NEVER fail: if filters are too strict, fall back to best available quotes.

NEW (Medium+):
- Persona-biased quote selection helper (get_persona_quotes), using existing category/axis.
  This lets Sully/Elena/Thorne naturally "disagree" by drawing different kinds of receipts.
"""

import re
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Sequence


# Prefer to filter these out, but never hard-fail if they appear.
DEFAULT_BAD_PHRASES = [
    "podcast",
    "subscribe",
    "welcome",
    "so glad you're here",
    "so glad you are here",
    "send that in",
    "leave a review",
    "like and subscribe",
    "download our app",
    "follow us",
    "visit our website",
    "new to the channel",
    "thanks for listening",
    "rate and review",
    "sign up",
    "weekly devotional",
    "devotional",
    "resource offer",
    "exclusive resource",
    "download",
    "visit",
    "follow",
    "leave a review",
    "rate and review",
    "new here",
    "get connected",
]

# Persona quote bias profiles: use existing category/axis dimensions.
# Keep this conservative: it nudges selection, it doesn't invent anything.
PERSONA_QUOTE_BIAS = {
    # Scripture receipts / "show me the text"
    "Sully": {
        "preferred_categories": ["scripture_reference", "doctrine"],
        "preferred_axes": ["scripture_vs_story"],
    },
    # Story/experience receipts / "what does this look like in real life"
    "Elena": {
        "preferred_categories": ["story", "experience"],
        "preferred_axes": ["doctrine_vs_experience"],
    },
    # Doctrine + caution receipts / "coherence, definitions, guardrails"
    "Thorne": {
        "preferred_categories": ["doctrine", "warning"],
        "preferred_axes": ["grace_vs_effort"],
    },
    # Default
    "Neutral": {
        "preferred_categories": [],
        "preferred_axes": [],
    },
}

_WORD_RE = re.compile(r"[a-z0-9']+")


def _since(days: Optional[int]) -> Optional[str]:
    if not days:
        return None
    dt = datetime.utcnow() - timedelta(days=days)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _normalize_for_dedupe(text: str) -> str:
    """Aggressive normalization for near-duplicate detection."""
    if not text:
        return ""
    t = text.lower()
    t = re.sub(r"\s+", " ", t).strip()
    # drop very common filler and punctuation-ish remnants
    t = re.sub(r"[^a-z0-9\s']+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _contains_bad_phrase(text: str, bad_phrases: Sequence[str]) -> bool:
    t = (text or "").lower()
    return any(p.lower() in t for p in bad_phrases)


def _trim_to_word_limit(text: str, max_words: int) -> str:
    if not text:
        return ""
    words = _WORD_RE.findall(text)
    if len(words) <= max_words:
        return re.sub(r"\s+", " ", text).strip()
    # take first max_words from the original text by scanning tokens
    # (simple approximation: rebuild from token list)
    trimmed = " ".join(words[:max_words])
    return trimmed.strip()


def get_quotes(
    conn: sqlite3.Connection,
    *,
    days: Optional[int] = None,
    limit: Optional[int] = None,
    category: Optional[str] = None,
    axis: Optional[str] = None,
    n: int = 3,
    distinct_channels: bool = True,
    max_words: int = 35,
    bad_phrases: Sequence[str] = DEFAULT_BAD_PHRASES,
    fetch_multiplier: int = 6,
) -> List[Dict]:
    """
    Returns up to n quote dicts:
      {video_id, channel_id, channel_name, title, published_at, axis, category, keyword, excerpt}

    Selection rules:
    - Prefer: evidence with non-null axis first, then earlier start_char.
    - Prefer: excludes promo-ish phrases (soft filter).
    - Prefer: distinct channels if possible.
    - De-dupe near-identical excerpts.
    - Fall back: if not enough clean quotes, include filtered ones.
    """

    if not category and not axis:
        raise ValueError("get_quotes requires category and/or axis")

    where = []
    params: List = []

    if category is not None:
        where.append("be.category = ?")
        params.append(category)

    if axis is not None:
        where.append("be.axis = ?")
        params.append(axis)

    since = _since(days)
    if since:
        where.append("v.published_at >= ?")
        params.append(since)

    where_sql = " AND ".join(where) if where else "1=1"

    # pull a bigger set and then filter in Python
    hard_limit = max(n * fetch_multiplier, n)
    if limit:
        hard_limit = min(hard_limit, int(limit))

    q = f"""
    SELECT
      be.video_id,
      v.channel_id,
      c.channel_name,
      v.title,
      v.published_at,
      be.axis,
      be.category,
      be.keyword,
      be.excerpt,
      be.start_char
    FROM brain_evidence be
    JOIN videos v ON be.video_id = v.video_id
    JOIN channels c ON v.channel_id = c.channel_id
    WHERE {where_sql}
    ORDER BY
      CASE WHEN be.axis IS NOT NULL THEN 0 ELSE 1 END,
      v.published_at DESC,
      be.start_char ASC
    LIMIT ?
    """
    rows = conn.execute(q, (*params, hard_limit)).fetchall()

    # Two-pass selection: clean-first, then fallback
    def select(pass_allow_bad: bool) -> List[Dict]:
        out: List[Dict] = []
        used_channels = set()
        seen = set()

        for r in rows:
            excerpt = (r["excerpt"] or "").strip()
            if not excerpt:
                continue

            is_bad = _contains_bad_phrase(excerpt, bad_phrases)
            if (not pass_allow_bad) and is_bad:
                continue

            norm = _normalize_for_dedupe(excerpt)
            # coarse fingerprint
            fp = (norm[:220] + f"|{r['category']}|{r['axis']}").strip()
            if fp in seen:
                continue

            ch = r["channel_id"]
            if distinct_channels and ch in used_channels:
                continue

            out.append({
                "video_id": r["video_id"],
                "channel_id": r["channel_id"],
                "channel_name": r["channel_name"],
                "title": r["title"],
                "published_at": r["published_at"],
                "axis": r["axis"],
                "category": r["category"],
                "keyword": r["keyword"],
                "excerpt": _trim_to_word_limit(excerpt, max_words=max_words),
            })

            seen.add(fp)
            used_channels.add(ch)
            if len(out) >= n:
                break

        return out

    clean = select(pass_allow_bad=False)
    if len(clean) >= n:
        return clean

    # fallback: allow “bad” phrases for remaining slots
    fallback = select(pass_allow_bad=True)
    # merge while preserving order, de-duped by (video_id, excerpt)
    merged: List[Dict] = []
    seen2 = set()
    for item in clean + fallback:
        key = (item["video_id"], item["excerpt"])
        if key in seen2:
            continue
        merged.append(item)
        seen2.add(key)
        if len(merged) >= n:
            break

    return merged


def get_persona_quotes(
    conn: sqlite3.Connection,
    persona: str = "Neutral",
    *,
    days: Optional[int] = None,
    n: int = 3,
    distinct_channels: bool = True,
    max_words: int = 35,
    bad_phrases: Sequence[str] = DEFAULT_BAD_PHRASES,
) -> List[Dict]:
    """
    Persona-biased quote selection.

    - Tries preferred categories first (in order), then preferred axes.
    - Soft-fails gracefully: if not enough results, falls back to neutral-safe pulls.
    - Uses existing get_quotes() logic, so you keep:
        diversity, dedupe, bad-phrase filtering + fallback.
    """

    bias = PERSONA_QUOTE_BIAS.get(persona, PERSONA_QUOTE_BIAS["Neutral"])

    # Helper to avoid ValueError from get_quotes (requires category or axis)
    def _try_category(cat: str) -> List[Dict]:
        return get_quotes(
            conn,
            days=days,
            category=cat,
            n=n,
            distinct_channels=distinct_channels,
            max_words=max_words,
            bad_phrases=bad_phrases,
        )

    def _try_axis(ax: str) -> List[Dict]:
        return get_quotes(
            conn,
            days=days,
            axis=ax,
            n=n,
            distinct_channels=distinct_channels,
            max_words=max_words,
            bad_phrases=bad_phrases,
        )

    collected: List[Dict] = []
    seen_keys = set()

    def _merge(items: List[Dict]):
        nonlocal collected, seen_keys
        for it in items:
            key = (it.get("video_id"), it.get("excerpt"))
            if key in seen_keys:
                continue
            collected.append(it)
            seen_keys.add(key)

    # 1) Preferred categories
    for cat in bias.get("preferred_categories", []):
        _merge(_try_category(cat))
        if len(collected) >= n:
            return collected[:n]

    # 2) Preferred axes
    for ax in bias.get("preferred_axes", []):
        _merge(_try_axis(ax))
        if len(collected) >= n:
            return collected[:n]

    # 3) If still short, broaden: try a couple common “safe” categories if present in your corpus.
    #    (If these categories don't exist, get_quotes will just return empty list.)
    for cat in ["scripture_reference", "doctrine", "story", "experience", "hope"]:
        _merge(_try_category(cat))
        if len(collected) >= n:
            return collected[:n]

    # 4) Absolute fallback: if persona is Neutral with no prefs, we still need receipts.
    #    We can't call get_quotes without category/axis, so pick a final default category.
    if not collected:
        _merge(_try_category("scripture_reference"))

    return collected[:n]
