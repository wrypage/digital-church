#!/usr/bin/env python3
"""
engine/climate_agenda.py

Builds a "climate agenda" pack from Brain outputs (JSON)
AND can render a clean, readable .docx report via engine/doc_writer.py.

New tuning:
  "What are our pastors trying to tell us?"

Usage:
  python -m engine.climate_agenda --days 30 --limit 120 --each 2
  python -m engine.climate_agenda --days 30 --limit 120 --each 2 --json
  python -m engine.climate_agenda --days 30 --limit 120 --each 2 --md
"""

import argparse
import json
import re
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from engine.doc_writer import write_doc
from engine.climate_snapshot import generate_climate_snapshot
from engine.config import DATABASE_PATH
from engine.quote_bank import get_quotes_for_video
from engine.elias_writer import write_elias_section


# ---------------------------
# Boilerplate backstop (Climate-side)
# ---------------------------
BOILERPLATE_PATTERNS = [
    r"\bnow (?:let\'?s|lets) dive into (?:today\'?s|this) (?:teaching|message)\b",
    r"\byou\'?re listening to\b",
    r"\bsubscribe\b",
    r"\bmarked by grace\b",
    r"\bfbcjacks\b",
    r"\bfbcjax\b",
    r"\bif you have a question\b",
    r"\bsend your question\b",
    r"\bhttps?://\S+\b",
    r"\bwww\.\S+\b",
    r"\b\S+\.(?:com|org|net|io|co|us|tv)\b",
    r"\bdot com\b",
    r"\bwith an x\b",
    r"\bsupport this ministry\b",
    r"\bto thank you for your support\b",
]
_BOILERPLATE_RE = re.compile("|".join(f"(?:{p})" for p in BOILERPLATE_PATTERNS), flags=re.IGNORECASE)


def is_boilerplate(text: str) -> bool:
    if not text:
        return False
    t = " ".join(text.strip().split())
    if not t:
        return False
    return bool(_BOILERPLATE_RE.search(t))


def _dedupe_quotes(quotes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out: List[Dict[str, Any]] = []
    for q in quotes or []:
        ex = " ".join((q.get("excerpt") or "").strip().lower().split())
        if not ex:
            continue
        if is_boilerplate(ex):
            continue
        if ex in seen:
            continue
        seen.add(ex)
        out.append(q)
    return out


# ---------------------------
# DB
# ---------------------------

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


def _norm_key(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


# ---------------------------
# Queries + aggregation
# ---------------------------

def fetch_period_sermons(conn: sqlite3.Connection, start_date: str, end_date: str) -> List[Dict[str, Any]]:
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

    items: List[Dict[str, Any]] = []
    for r in rows:
        raw = _safe_json_load(r["raw_scores_json"], {})
        zscores = (raw.get("zscores", {}) or {}).get("axes", {})
        drift_level = raw.get("drift_level", "unknown")

        drift_mag = 0.0
        if zscores:
            try:
                drift_mag = max(abs(float(z)) for z in zscores.values())
            except Exception:
                drift_mag = 0.0

        tone_profile = raw.get("tone_profile", {}) or {}
        tone_tags = (tone_profile.get("dominant_tone_tags") or []) if isinstance(tone_profile, dict) else []

        items.append({
            "video_id": r["video_id"],
            "channel_id": r["channel_id"],
            "channel_name": r["channel_name"],
            "title": r["title"],
            "published_at": r["published_at"],
            "theological_density": float(r["theological_density"] or 0.0),
            "axis_scores": {
                "grace_vs_effort": float(r["grace_vs_effort"] or 0.0),
                "hope_vs_fear": float(r["hope_vs_fear"] or 0.0),
                "doctrine_vs_experience": float(r["doctrine_vs_experience"] or 0.0),
                "scripture_vs_story": float(r["scripture_vs_story"] or 0.0),
            },
            "category_density": (raw.get("category_density") or raw.get("raw_scores_json") or {}).get("category_density")
            if isinstance(raw, dict) else None,
            "intent_vectors": (raw.get("intent_vectors") or {}) if isinstance(raw, dict) else {},
            "tone_tags": tone_tags,
            "drift_level": drift_level,
            "drift_magnitude": float(drift_mag or 0.0),
            "raw": raw,
        })

    return items


def identify_theme_convergence(items: List[Dict[str, Any]], top_n: int = 3) -> List[Dict[str, Any]]:
    # Theme convergence uses category_density from raw_scores_json.category_density if available
    sums: Dict[str, float] = {}
    n_sermons = len(items) or 1
    for it in items:
        raw = it.get("raw") or {}
        dens = (raw.get("category_density") or {}) if isinstance(raw, dict) else {}
        if not dens:
            dens = (raw.get("raw_scores_json", {}) or {}).get("category_density", {}) if isinstance(raw, dict) else {}
        if not isinstance(dens, dict):
            continue
        for k, v in dens.items():
            try:
                sums[k] = sums.get(k, 0.0) + float(v)
            except Exception:
                continue

    top = sorted(sums.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
    out = []
    for theme, total in top:
        out.append({
            "theme": theme,
            "total_density": round(float(total), 1),
            "sermon_count": int(n_sermons),
            "avg_density": round(float(total) / n_sermons, 2),
        })
    return out


def identify_scripture_focus(items: List[Dict[str, Any]], top_n: int = 5) -> List[Dict[str, Any]]:
    # Scripture references live in raw_scores_json.scripture_refs
    totals: Dict[str, int] = {}
    sermon_counts: Dict[str, int] = {}

    for it in items:
        raw = it.get("raw") or {}
        refs = (raw.get("scripture_refs") or {}) if isinstance(raw, dict) else {}
        if not isinstance(refs, dict):
            continue
        for book, count in refs.items():
            try:
                c = int(count)
            except Exception:
                continue
            totals[book] = totals.get(book, 0) + c
            sermon_counts[book] = sermon_counts.get(book, 0) + 1

    top = sorted(totals.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
    out = []
    for book, total_refs in top:
        sc = sermon_counts.get(book, 0) or 1
        out.append({
            "book": book,
            "total_references": int(total_refs),
            "sermon_count": int(sc),
            "avg_refs_per_sermon": round(float(total_refs) / sc, 1),
        })
    return out


def select_resonant_sermons(
    conn: sqlite3.Connection,
    items: List[Dict[str, Any]],
    themes: List[Dict[str, Any]],
    limit_each: int = 2
) -> List[Dict[str, Any]]:
    # pick top themed sermons by their category density
    out: List[Dict[str, Any]] = []
    theme_keys = [t.get("theme") for t in themes if t.get("theme")]

    # Build lookup of densities by video_id
    by_id: Dict[str, Dict[str, float]] = {}
    for it in items:
        raw = it.get("raw") or {}
        dens = (raw.get("category_density") or {}) if isinstance(raw, dict) else {}
        if not isinstance(dens, dict):
            dens = {}
        by_id[it.get("video_id")] = {k: float(v) for k, v in dens.items() if isinstance(v, (int, float)) or str(v).replace(".", "", 1).isdigit()}

    for theme in theme_keys:
        # rank by density for this theme
        ranked = []
        for it in items:
            vid = it.get("video_id")
            dens = by_id.get(vid, {})
            score = float(dens.get(theme, 0.0))
            ranked.append((score, it))
        ranked.sort(key=lambda x: x[0], reverse=True)

        for score, it in ranked[: max(1, int(limit_each))]:
            vid = it.get("video_id")
            # quote_bank API varies (some versions use n=, not limit=)
            try:
                quotes = get_quotes_for_video(conn, vid, category=theme, limit=6)
            except TypeError:
                try:
                    quotes = get_quotes_for_video(conn, vid, category=theme, n=6)
                except TypeError:
                    # last resort: positional
                    quotes = get_quotes_for_video(conn, vid, theme, 6)
            quotes = _dedupe_quotes(quotes)

            out.append({
                "video_id": vid,
                "channel_name": it.get("channel_name"),
                "title": it.get("title"),
                "published_at": it.get("published_at"),
                "reason": f"High '{theme}' density",
                "theological_density": it.get("theological_density"),
                "category_density": float(score),
                "quote_bank": quotes[:3],
            })

    return out


def select_outliers(items: List[Dict[str, Any]], top_n: int = 3) -> List[Dict[str, Any]]:
    ranked = sorted(items, key=lambda it: float(it.get("drift_magnitude") or 0.0), reverse=True)[:top_n]
    out = []
    for it in ranked:
        out.append({
            "video_id": it.get("video_id"),
            "channel_name": it.get("channel_name"),
            "title": it.get("title"),
            "published_at": it.get("published_at"),
            "drift_level": it.get("drift_level"),
            "drift_magnitude": round(float(it.get("drift_magnitude") or 0.0), 2),
            "axis_scores": it.get("axis_scores") or {},
        })
    return out


# ---------------------------
# Intent climate v2 (with boilerplate backstop + cross-channel gating)
# ---------------------------

def build_intent_climate_v2(items: List[Dict[str, Any]], window_key: str, days: int) -> Dict[str, Any]:
    """
    Aggregate 'intent_vectors' across sermons to answer:
      What are our pastors trying to tell us?

    Refinement:
      - Semantic bucketing: cluster intent strings by meaning (regex buckets)
      - Cross-channel gating: require repetition across multiple channels
      - Output structure unchanged
    """
    total_sermons = len(items)
    channels = {it.get("channel_id") for it in items if it.get("channel_id")}
    channels_included = len(channels)

    # -----------------------------
    # Signal gating (keep discipline)
    # -----------------------------
    MIN_UNIQUE_CHANNELS = 2   # minimum distinct channels that must repeat the item
    MIN_MENTIONS = 2          # minimum distinct sermons that must mention the item

    # -----------------------------
    # Boilerplate / stopword utilities
    # -----------------------------
    STOPWORDS = {
        "the","a","an","and","or","but","to","of","in","on","for","with","as","at","by","from",
        "that","this","it","is","are","was","were","be","been","being","we","you","your","our",
        "they","their","he","she","his","her","them","i","me","my","mine","us","so","not","no",
        "do","does","did","will","would","can","could","should","may","might","must",
    }

    def _canonical_words(text: str) -> str:
        t = (text or "").strip().lower()
        t = re.sub(r"[^\w\s]", " ", t)
        t = re.sub(r"\s+", " ", t).strip()
        if not t:
            return ""
        parts = [p for p in t.split(" ") if p and p not in STOPWORDS]
        t2 = " ".join(parts)
        t2 = re.sub(r"\s+", " ", t2).strip()
        return t2 or t

    # -----------------------------
    # Semantic buckets (meaning-level clustering)
    # -----------------------------
    # Keep these conservative. We can expand later based on what emerges.
    BUCKETS = [
        # Dependence / trust vs self-reliance
        ("depend_on_god", "Depend on God over self-reliance",
         [
             r"\btrust (?:in )?god\b",
             r"\brely (?:on|upon) (?:the )?lord\b",
             r"\bdepend (?:on|upon) (?:the )?lord\b",
             r"\bnot (?:lean|rely) on (?:your|our|their) own\b",
             r"\bown understanding\b",
             r"\bself[- ]reliance\b",
             r"\bself[- ]sufficient\b",
             r"\bhuman strength\b",
         ]),

        # Repentance / holiness / obedience
        ("repent_obey", "Repentance, holiness, and obedience",
         [
             r"\brepent(?:ance)?\b",
             r"\bturn from sin\b",
             r"\bconfess(?:ion)?\b",
             r"\bconvict(?:ion)? of sin\b",
             r"\bholiness\b",
             r"\bobedien(?:ce|t)\b",
             r"\bsubmit(?:ting)? to (?:god|christ|the lord)\b",
         ]),

        # Assurance / grace vs striving
        ("assurance_grace", "Assurance in Christ over striving",
         [
             r"\bassurance\b",
             r"\bgrace\b.*\bnot\b.*\bworks\b",
             r"\bnot by works\b",
             r"\bearn(?:ing)?\b.*\b(?:salvation|favor)\b",
             r"\bstriv(?:e|ing)\b",
             r"\bperform(?:ance)?\b",
         ]),

        # Prayer / seeking God
        ("prayer_seek", "Prayer and seeking God",
         [
             r"\bprayer\b",
             r"\bpray\b",
             r"\bseek (?:the )?lord\b",
             r"\bseek god\b",
             r"\bfast(?:ing)?\b",
             r"\bcry out\b",
         ]),

        # Unity / division / church health
        ("unity_church", "Unity and church health",
         [
             r"\bunity\b",
             r"\bdivision\b",
             r"\bbody of christ\b",
             r"\bone another\b",
             r"\bchurch\b.*\b(?:together|family|community)\b",
         ]),

        # Fear / anxiety / courage / hope
        ("fear_anxiety", "Fear, anxiety, and courage in Christ",
         [
             r"\bfear\b",
             r"\banxiety\b",
             r"\bworry\b",
             r"\bpanic\b",
             r"\bafraid\b",
             r"\bcourage\b",
             r"\bdo not fear\b",
         ]),

        # Gospel / evangelism / mission
        ("gospel_mission", "Gospel proclamation and mission",
         [
             r"\bgospel\b",
             r"\bevangel(?:ism|ize|istic)\b",
             r"\bshare (?:your|the) faith\b",
             r"\bwitness\b",
             r"\bmission(?:s)?\b",
             r"\bmake disciples\b",
         ]),

        # Scripture authority / truth / discernment
        ("scripture_truth", "Scripture, truth, and discernment",
         [
             r"\bscripture\b",
             r"\bword of god\b",
             r"\bbible\b",
             r"\btruth\b",
             r"\bdiscern(?:ment)?\b",
             r"\bfalse teaching\b",
             r"\bdoctrine\b",
         ]),

        # End times / prophecy / Israel (keep narrow)
        ("prophecy_endtimes", "Prophecy and end-times emphasis",
         [
             r"\bprophecy\b",
             r"\bend times\b",
             r"\btribulation\b",
             r"\bantichrist\b",
             r"\bmillennium\b",
             r"\bisrael\b",
             r"\bsecond coming\b",
             r"\bchrist(?:'s)? return\b",
         ]),
    ]

    BUCKET_LABEL = {bid: label for (bid, label, _patterns) in BUCKETS}
    BUCKET_RES = [(bid, re.compile("|".join(f"(?:{p})" for p in patterns), flags=re.IGNORECASE))
                  for (bid, _label, patterns) in BUCKETS]

    def _bucket_id(text: str) -> str:
        t = (text or "").strip()
        if not t:
            return ""
        if is_boilerplate(t):
            return ""
        for bid, rx in BUCKET_RES:
            try:
                if rx.search(t):
                    return bid
            except Exception:
                continue
        return ""

    def _canonical_key(text: str) -> str:
        """
        First try a semantic bucket id; otherwise fall back to word-canonicalization.
        """
        bid = _bucket_id(text)
        if bid:
            return f"bucket::{bid}"
        return _canonical_words(text)

    def _display_text(canon: str, original: str) -> str:
        """
        If bucketed, show bucket label. Else show original (trimmed).
        """
        if canon.startswith("bucket::"):
            bid = canon.split("bucket::", 1)[1].strip()
            return BUCKET_LABEL.get(bid, bid)
        return (original or "").strip()

    # Weighted counts + supporting examples
    msg_counts: Dict[str, float] = {}
    msg_examples: Dict[str, List[Dict[str, Any]]] = {}

    warn_counts: Dict[str, float] = {}
    warn_examples: Dict[str, List[Dict[str, Any]]] = {}

    enc_counts: Dict[str, float] = {}
    enc_examples: Dict[str, List[Dict[str, Any]]] = {}

    cta_counts: Dict[str, float] = {}
    cta_examples: Dict[str, List[Dict[str, Any]]] = {}

    concern_counts: Dict[str, float] = {}
    concern_examples: Dict[str, List[Dict[str, Any]]] = {}

    # Track repetition across channels/sermons
    msg_channels: Dict[str, set] = {}
    msg_sermons: Dict[str, set] = {}

    warn_channels: Dict[str, set] = {}
    warn_sermons: Dict[str, set] = {}

    enc_channels: Dict[str, set] = {}
    enc_sermons: Dict[str, set] = {}

    cta_channels: Dict[str, set] = {}
    cta_sermons: Dict[str, set] = {}

    concern_channels: Dict[str, set] = {}
    concern_sermons: Dict[str, set] = {}

    # Keep a representative "display" per canon
    display_msg: Dict[str, str] = {}
    display_warn: Dict[str, str] = {}
    display_enc: Dict[str, str] = {}
    display_cta: Dict[str, str] = {}
    display_concern: Dict[str, str] = {}

    tone_tag_counts: Dict[str, float] = {}

    axis_sum = {
        "hope_vs_fear": 0.0,
        "grace_vs_effort": 0.0,
        "scripture_vs_story": 0.0,
        "doctrine_vs_experience": 0.0,
    }
    axis_n = 0

    def _register_display(display_map: Dict[str, str], canon: str, original: str) -> None:
        if not canon:
            return
        if canon in display_map:
            return
        disp = _display_text(canon, original)
        if disp:
            display_map[canon] = disp

    def _track(canon: str, it: Dict[str, Any], channels_map: Dict[str, set], sermons_map: Dict[str, set]) -> None:
        if not canon:
            return
        channels_map.setdefault(canon, set())
        sermons_map.setdefault(canon, set())
        cid = it.get("channel_id")
        vid = it.get("video_id")
        if cid:
            channels_map[canon].add(cid)
        if vid:
            sermons_map[canon].add(vid)

    def _add_example(store: Dict[str, List[Dict[str, Any]]], canon: str, it: Dict[str, Any], excerpt: str) -> None:
        store.setdefault(canon, [])
        if len(store[canon]) >= 3:
            return
        ex = (excerpt or "").strip()
        if not ex or is_boilerplate(ex):
            return
        store[canon].append({
            "sermon_id": it.get("video_id"),
            "channel_name": it.get("channel_name"),
            "speaker": it.get("speaker", ""),
            "published_at": it.get("published_at"),
            "excerpt": ex,
        })

    for it in items:
        axis = it.get("axis_scores") or {}
        if axis:
            for k in axis_sum.keys():
                axis_sum[k] += float(axis.get(k, 0.0) or 0.0)
            axis_n += 1

        for t in (it.get("tone_tags") or []):
            tone_tag_counts[t] = tone_tag_counts.get(t, 0.0) + 1.0

        raw_intent = it.get("intent_vectors") or {}

        # ---- Primary burden ----
        primary = raw_intent.get("primary_burden") or {}
        summary = (primary.get("summary") or "").strip()
        conf = float(primary.get("confidence") or 0.0)

        if summary and not is_boilerplate(summary):
            canon = _canonical_key(summary)
            if canon and not is_boilerplate(canon):
                _register_display(display_msg, canon, summary)
                msg_counts[canon] = msg_counts.get(canon, 0.0) + max(conf, 0.25)
                _track(canon, it, msg_channels, msg_sermons)
                ev = primary.get("evidence") or []
                excerpt = ev[0].get("excerpt") if ev else ""
                _add_example(msg_examples, canon, it, excerpt)

        # ---- Secondary burdens ----
        for sec in (raw_intent.get("secondary_burdens") or []):
            summ = (sec.get("summary") or "").strip()
            c = float(sec.get("confidence") or 0.0)
            if summ and not is_boilerplate(summ):
                canon = _canonical_key(summ)
                if canon and not is_boilerplate(canon):
                    _register_display(display_msg, canon, summ)
                    msg_counts[canon] = msg_counts.get(canon, 0.0) + max(c, 0.15)
                    _track(canon, it, msg_channels, msg_sermons)
                    ev = sec.get("evidence") or []
                    excerpt = ev[0].get("excerpt") if ev else ""
                    _add_example(msg_examples, canon, it, excerpt)

        # ---- Warnings ----
        for w in (raw_intent.get("warnings") or []):
            txt = (w.get("warning") or "").strip()
            if not txt or is_boilerplate(txt):
                continue
            canon = _canonical_key(txt)
            if not canon or is_boilerplate(canon):
                continue
            _register_display(display_warn, canon, txt)
            warn_counts[canon] = warn_counts.get(canon, 0.0) + max(float(w.get("confidence") or 0.0), 0.25)
            _track(canon, it, warn_channels, warn_sermons)
            ev = w.get("evidence") or []
            excerpt = ev[0].get("excerpt") if ev else txt
            _add_example(warn_examples, canon, it, excerpt)

        # ---- Encouragements ----
        for e in (raw_intent.get("encouragements") or []):
            txt = (e.get("encouragement") or "").strip()
            if not txt or is_boilerplate(txt):
                continue
            canon = _canonical_key(txt)
            if not canon or is_boilerplate(canon):
                continue
            _register_display(display_enc, canon, txt)
            enc_counts[canon] = enc_counts.get(canon, 0.0) + max(float(e.get("confidence") or 0.0), 0.25)
            _track(canon, it, enc_channels, enc_sermons)
            ev = e.get("evidence") or []
            excerpt = ev[0].get("excerpt") if ev else txt
            _add_example(enc_examples, canon, it, excerpt)

        # ---- Calls to action ----
        for cta in (raw_intent.get("calls_to_action") or []):
            txt = (cta.get("action") or "").strip()
            if not txt or is_boilerplate(txt):
                continue
            canon = _canonical_key(txt)
            if not canon or is_boilerplate(canon):
                continue
            _register_display(display_cta, canon, txt)
            cta_counts[canon] = cta_counts.get(canon, 0.0) + max(float(cta.get("confidence") or 0.0), 0.25)
            _track(canon, it, cta_channels, cta_sermons)
            ev = cta.get("evidence") or []
            excerpt = ev[0].get("excerpt") if ev else txt
            _add_example(cta_examples, canon, it, excerpt)

        # ---- Underlying concerns ----
        for c in (raw_intent.get("assumed_concerns") or []):
            txt = (c.get("concern") or "").strip()
            if not txt or is_boilerplate(txt):
                continue
            canon = _canonical_key(txt)
            if not canon or is_boilerplate(canon):
                continue
            _register_display(display_concern, canon, txt)
            concern_counts[canon] = concern_counts.get(canon, 0.0) + max(float(c.get("confidence") or 0.0), 0.2)
            _track(canon, it, concern_channels, concern_sermons)
            ev = c.get("evidence") or []
            excerpt = ev[0].get("excerpt") if ev else txt
            _add_example(concern_examples, canon, it, excerpt)

    def _filter_counts(counts: Dict[str, float], channels_map: Dict[str, set], sermons_map: Dict[str, set]) -> Dict[str, float]:
        elig = set()
        for k in counts.keys():
            if len(channels_map.get(k, set())) >= MIN_UNIQUE_CHANNELS and len(sermons_map.get(k, set())) >= MIN_MENTIONS:
                elig.add(k)
        return {k: v for k, v in counts.items() if k in elig}

    # Apply gating
    msg_counts_f = _filter_counts(msg_counts, msg_channels, msg_sermons)
    warn_counts_f = _filter_counts(warn_counts, warn_channels, warn_sermons)
    enc_counts_f = _filter_counts(enc_counts, enc_channels, enc_sermons)
    cta_counts_f = _filter_counts(cta_counts, cta_channels, cta_sermons)
    concern_counts_f = _filter_counts(concern_counts, concern_channels, concern_sermons)

    def _top(d: Dict[str, float], n: int, display_map: Dict[str, str]) -> List[Dict[str, Any]]:
        total_w = sum(d.values()) or 1.0
        rows = sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:n]
        out: List[Dict[str, Any]] = []
        for canon, w in rows:
            out.append({
                "key": canon,
                "text": display_map.get(canon, canon),
                "share": round(float(w / total_w), 3),
                "weight": round(float(w), 3),
            })
        return out

    def _items_with_examples(
        top_list: List[Dict[str, Any]],
        examples: Dict[str, List[Dict[str, Any]]],
        key_name: str
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for row in top_list:
            canon = row["key"]
            txt = row["text"]
            if not txt or is_boilerplate(txt):
                continue
            out.append({
                key_name: txt,
                "share": row["share"],
                "confidence": min(0.95, 0.55 + row["share"]),
                "evidence": examples.get(canon, []),
            })
        return out

    top_msgs = _top(msg_counts_f, 3, display_msg)
    top_warns = _top(warn_counts_f, 2, display_warn)
    top_encs = _top(enc_counts_f, 2, display_enc)
    top_ctas = _top(cta_counts_f, 3, display_cta)
    top_concerns = _top(concern_counts_f, 2, display_concern)

    axis_avg = {k: round(axis_sum[k] / max(axis_n, 1), 3) for k in axis_sum.keys()}

    tone_total = sum(tone_tag_counts.values()) or 1.0
    tone_ranked = sorted(tone_tag_counts.items(), key=lambda kv: kv[1], reverse=True)[:6]
    tone_out = [{"tag": k, "share": round(float(v / tone_total), 3)} for k, v in tone_ranked]

    # Headline selection:
    # Prefer repeated (bucketed) burdens; else fall back to tone/axes.
    headline = top_msgs[0]["text"] if top_msgs else ""
    if not headline:
        top_tone = tone_out[0]["tag"] if tone_out else "mixed"
        gvse = float(axis_avg.get("grace_vs_effort", 0.0))
        hvf = float(axis_avg.get("hope_vs_fear", 0.0))

        axis_bits = []
        axis_bits.append("effort-forward" if gvse < -0.10 else "grace-forward" if gvse > 0.10 else "balanced on effort/grace")
        axis_bits.append("hope-leaning" if hvf > 0.10 else "warning-leaning" if hvf < -0.10 else "balanced on hope/fear")

        headline = f"Network tone: {top_tone}; {', '.join(axis_bits[:2])}."

    summary_bits = []
    if headline:
        summary_bits.append(f"Top burden: {headline}")
    if top_warns:
        summary_bits.append(f"Warnings: {top_warns[0]['text']}")
    if top_ctas:
        summary_bits.append(f"Calls to action: {top_ctas[0]['text']}")
    summary = " | ".join(summary_bits)

    climate = {
        "time_window": {
            "key": window_key,
            "days": int(days),
            "sermons_included": int(total_sermons),
            "channels_included": int(channels_included),
        },
        "the_message_this_window": {
            "headline": headline,
            "summary": summary,
            "confidence": 0.65 if headline else 0.0,
        },
        "primary_messages_being_pressed": _items_with_examples(top_msgs, msg_examples, "message"),
        "warnings_repeated": _items_with_examples(top_warns, warn_examples, "warning"),
        "encouragements_amplified": _items_with_examples(top_encs, enc_examples, "encouragement"),
        "calls_to_action_most_urged": _items_with_examples(top_ctas, cta_examples, "action"),
        "underlying_concerns_implied_by_repetition": _items_with_examples(top_concerns, concern_examples, "concern"),
        "tone_and_atmosphere": {
            "tone_tags_ranked": tone_out,
            "axes_avg": axis_avg,
        },
        "questions_for_leaders": [
            "If these burdens are accurate, what formation practices (Word, prayer, community) need strengthening right now?",
            "Which warnings are repeated most—and do our people know how to respond without panic?",
            "What does the dominant tone suggest about congregational emotional weather in this season?",
            "Where are pastors calling for action—personal repentance, corporate unity, mission—and what support is required?",
            "Are we hearing one shared message across channels, or distinct local burdens requiring local responses?",
        ],
        "method_notes": {
            "how_inferred": (
                "Intent inferred from Brain intent_vectors grounded in explicit language and repeated emphasis. "
                "Climate filters out obvious bumpers/branding/URLs. "
                "Climate v2 also performs conservative semantic bucketing (meaning-level clustering) and requires "
                "cross-channel repetition (min unique channels and mentions) before elevating a burden to the network level."
            ),
        }
    }

    return {"climate_v2": climate}


# ---------------------------
# Elias pack (compatible with your current elias_writer.py)
# ---------------------------

def _build_elias_pack(
    conn: sqlite3.Connection,
    snapshot: Dict[str, Any],
    themes: List[Dict[str, Any]],
    scripture: List[Dict[str, Any]],
    days: int,
    quotes_per_section: int = 3
) -> Dict[str, Any]:
    lines = write_elias_section(
        conn=conn,
        climate_snapshot=snapshot,
        theme_convergence=themes,
        scripture_focus=scripture,
        days=days,
        quotes_per_section=quotes_per_section
    )

    return {
        "preface": "",
        "observations": lines,
        "closing_style": "",
        "closing_line": "",
    }


# ---------------------------
# Core: JSON agenda builder
# ---------------------------

def generate_climate_agenda(days: int = 30, limit: int = 120, limit_each: int = 2) -> Dict[str, Any]:
    conn = _connect()

    snapshot = generate_climate_snapshot(days=days)

    now = datetime.utcnow()
    current_end = now.strftime("%Y-%m-%d %H:%M:%S")
    current_start = (now - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    window_key = f"{current_start.split(' ')[0]}..{current_end.split(' ')[0]}"

    items = fetch_period_sermons(conn, current_start, current_end)
    if limit and len(items) > limit:
        items = items[:limit]

    themes = identify_theme_convergence(items, top_n=3)
    scripture = identify_scripture_focus(items, top_n=5)

    intent_climate_v2 = build_intent_climate_v2(items, window_key=window_key, days=days)

    generated_at_iso = datetime.utcnow().isoformat() + "Z"

    elias_pack = _build_elias_pack(
        conn=conn,
        snapshot=snapshot,
        themes=themes,
        scripture=scripture,
        days=days,
        quotes_per_section=3
    )

    resonant = select_resonant_sermons(conn, items, themes, limit_each=limit_each)
    outliers = select_outliers(items, top_n=3)

    agenda = {
        "climate_snapshot": snapshot,
        "intent_climate_v2": intent_climate_v2,
        "theme_convergence": themes,
        "scripture_focus": scripture,
        "elias_preface": elias_pack.get("preface", ""),
        "observations": elias_pack.get("observations", []),
        "elias_closing_style": elias_pack.get("closing_style", ""),
        "elias_closing_line": elias_pack.get("closing_line", ""),
        "resonant_sermons": resonant,
        "outliers": outliers,
        "metadata": {
            "days": int(days),
            "limit": int(limit),
            "total_sermons": len(items),
            "generated_at": generated_at_iso,
            "window_key": window_key,
        }
    }

    return agenda


# ---------------------------
# CLI
# ---------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--limit", type=int, default=120)
    ap.add_argument("--each", type=int, default=2)
    ap.add_argument("--json", action="store_true", help="Write JSON only")
    ap.add_argument("--md", action="store_true", help="Write Markdown only (no docx)")
    args = ap.parse_args()

    agenda = generate_climate_agenda(days=args.days, limit=args.limit, limit_each=args.each)

    # Always print JSON to stdout (stable behavior)
    print(json.dumps(agenda, indent=2))

    # Also write files (stable behavior)
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_json = f"out/climate_agenda_{stamp}.json"
    with open(out_json, "w", encoding="utf-8") as f:
        f.write(json.dumps(agenda, indent=2))

    if args.md:
        # Minimal markdown export (doc_writer handles nicer docx)
        out_md = f"out/climate_agenda_{stamp}.md"
        with open(out_md, "w", encoding="utf-8") as f:
            f.write("# Climate Agenda\n\n")
            f.write(f"- Window: {agenda['metadata']['window_key']} ({agenda['metadata']['days']} days)\n")
            f.write(f"- Sermons included: {agenda['metadata']['total_sermons']}\n\n")
            f.write("## Elias\n\n")
            for line in agenda.get("observations") or []:
                f.write(line + "\n")
        return

    if not args.json:
        out_docx = f"out/climate_agenda_{stamp}.docx"
        write_doc(agenda, out_docx)


if __name__ == "__main__":
    main()