#!/usr/bin/env python3
"""
engine/assembly_run.py

Turns climate_agenda JSON into Substack-ready Markdown.

Key features:
- Two-paragraph narrative lead (human, intentional)
- Curated "Striking receipts" (3–5 quotes)
- Elias observations always present and readable
- Clean formatting (no raw dict printing)
- NEW: Quote quality gate to avoid garbled ASR excerpts
"""

import argparse
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ----------------------------
# Helpers
# ----------------------------

THEO_KEYWORDS = {
    "jesus", "christ", "cross", "resurrection", "gospel", "kingdom",
    "repent", "grace", "mercy", "faith", "spirit", "holy", "scripture",
    "word", "truth", "sin", "forgive", "covenant", "glory", "lord"
}

# Patterns that often indicate garbled ASR (missing leading consonants, etc.)
GARBLED_MARKERS = [
    " oly ",     # "Holy"
    " pirit",    # "Spirit"
    " esus",     # "Jesus"
    " astor ",   # "Pastor"
    " onnect ",  # "Connect"
    " ith ",     # "with"
    " e's ",     # "he's" / "she's" missing first letter
    " e ",       # risky, but used in combo logic below
]


def _slugify(text: str, max_len: int = 90) -> str:
    text = (text or "").strip().lower()
    text = re.sub(r"[^a-z0-9\s-]", "", text)
    text = re.sub(r"\s+", "-", text).strip("-")
    return text[:max_len] if text else "draft"


def _title_case(s: str) -> str:
    s = (s or "").replace("_", " ").strip()
    if not s:
        return s
    return s[0].upper() + s[1:]


def _safe_list(x) -> List:
    return x if isinstance(x, list) else []


def _safe_dict(x) -> Dict:
    return x if isinstance(x, dict) else {}


def _fmt_num(x) -> str:
    try:
        fx = float(x)
        if abs(fx - round(fx)) < 1e-9:
            return str(int(round(fx)))
        return f"{fx:.2f}".rstrip("0").rstrip(".")
    except Exception:
        return str(x)


# ----------------------------
# Pull data from agenda JSON
# ----------------------------

def _pick_title(agenda: Dict[str, Any]) -> str:
    themes = _safe_list(agenda.get("theme_convergence"))
    books = _safe_list(agenda.get("scripture_focus"))

    top_theme = themes[0].get("theme") if themes and isinstance(themes[0], dict) else None
    top_book = books[0].get("book") if books and isinstance(books[0], dict) else None

    if top_theme and top_book:
        return f"This Week’s Signals: {_title_case(top_theme)} + {_title_case(top_book)}"
    if top_theme:
        return f"This Week’s Signals: {_title_case(top_theme)}"
    if top_book:
        return f"This Week’s Signals in {_title_case(top_book)}"
    return "This Week in the Digital Pulpit"


def _extract_snapshot(agenda: Dict[str, Any]) -> Dict[str, Any]:
    # agenda["climate_snapshot"] is the full snapshot output
    return _safe_dict(agenda.get("climate_snapshot"))


def _extract_current_period(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    cs = _safe_dict(snapshot.get("climate_snapshot"))
    return _safe_dict(cs.get("current"))


def _extract_drift_rate(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    cs = _safe_dict(snapshot.get("climate_snapshot"))
    return _safe_dict(cs.get("drift_rate"))


def _extract_themes(agenda: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [t for t in _safe_list(agenda.get("theme_convergence")) if isinstance(t, dict)]


def _extract_books(agenda: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [b for b in _safe_list(agenda.get("scripture_focus")) if isinstance(b, dict)]


def _extract_observations(agenda: Dict[str, Any]) -> List[Dict[str, Any]]:
    # observation objects use keys: speaker, statement, quote_bank
    obs = _safe_list(agenda.get("observations"))
    out = []
    for o in obs:
        if isinstance(o, dict):
            out.append(o)
        elif isinstance(o, str):
            out.append({"speaker": "Elias", "statement": o, "quote_bank": []})
    return out


def _extract_resonant(agenda: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = _safe_list(agenda.get("resonant_sermons"))
    return [x for x in items if isinstance(x, dict)]


def _extract_outliers(agenda: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = _safe_list(agenda.get("outliers"))
    return [x for x in items if isinstance(x, dict)]


# ----------------------------
# Receipts: scoring + formatting
# ----------------------------

def _normalize_quote_text(q: str) -> str:
    q = (q or "").strip()
    q = re.sub(r"\s+", " ", q)
    return q


def _is_garbled_asr(text: str) -> bool:
    """
    Detect quotes that look like missing leading letters or heavily mangled ASR.
    This is a heuristic "quality gate" so Substack quotes stay readable.
    """
    t = f" {_normalize_quote_text(text).lower()} "
    if not t.strip():
        return True

    # 1) Too many 1-letter tokens often indicates shredded transcription
    tokens = re.findall(r"[a-z']+", t)
    if tokens:
        one_letter = sum(1 for w in tokens if len(w) == 1)
        if one_letter / max(len(tokens), 1) > 0.08:
            return True

    # 2) Common missing-first-letter markers (require >=2 hits to avoid false positives)
    marker_hits = 0
    for m in GARBLED_MARKERS:
        if m in t:
            marker_hits += 1
    if marker_hits >= 2:
        return True

    # 3) Weird density of truncated apostrophes like "e's"
    if t.count("e's") >= 2:
        return True

    return False


def _quote_score(text: str) -> float:
    """
    Heuristic "striking" score:
    - longer (to a point) is better
    - punctuation energy helps
    - theological keywords boost
    - promo language penalized
    - NEW: quality gate rejects garbled ASR excerpts
    """
    t_raw = _normalize_quote_text(text)
    if not t_raw:
        return 0.0

    # NEW: reject mangled quotes (keeps output readable)
    if _is_garbled_asr(t_raw):
        return 0.0

    t = f" {t_raw.lower()} "
    length = len(t)

    # sweet spot around 140–260 chars
    if length <= 80:
        length_score = length / 80.0
    elif length <= 240:
        length_score = 1.0 + (length - 80) / 160.0
    else:
        length_score = 2.0 - min((length - 240) / 400.0, 0.9)

    punct = 0.0
    punct += 0.4 if "?" in t else 0.0
    punct += 0.3 if "!" in t else 0.0
    punct += 0.2 if ":" in t else 0.0

    tokens = set(re.findall(r"[a-z']+", t))
    kw_hits = len(tokens.intersection(THEO_KEYWORDS))
    kw = min(kw_hits * 0.12, 0.8)

    promo_penalty = 0.0
    for bad in ("subscribe", "podcast", "like and subscribe", "download our app", "follow us", "visit our website"):
        if bad in t:
            promo_penalty += 0.6

    return max(0.0, length_score + punct + kw - promo_penalty)


def _format_receipt_block(q: Dict[str, Any]) -> str:
    quote = _normalize_quote_text(q.get("quote") or q.get("excerpt") or "")
    if not quote:
        return ""

    channel = (q.get("channel_name") or q.get("channel") or "").strip()
    title = (q.get("title") or q.get("video_title") or "").strip()
    url = (q.get("url") or q.get("video_url") or "").strip()

    lines = [f"> {quote}"]
    attrib_parts = []
    if title:
        attrib_parts.append(f"“{title}”")
    if channel:
        attrib_parts.append(channel)
    attrib = " — ".join(attrib_parts).strip()
    if attrib:
        if url:
            lines.append(f"> \n> *{attrib}* ({url})")
        else:
            lines.append(f"> \n> *{attrib}*")

    return "\n".join(lines).strip()


def _collect_all_receipts(observations: List[Dict[str, Any]], resonant: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    receipts: List[Dict[str, Any]] = []

    for ob in observations:
        qb = ob.get("quote_bank") or []
        if isinstance(qb, list):
            for r in qb:
                if isinstance(r, dict):
                    receipts.append(r)

    for rs in resonant:
        qb = rs.get("quote_bank") or []
        if isinstance(qb, list):
            for r in qb:
                if isinstance(r, dict):
                    receipts.append(r)

    # de-dupe by quote text
    seen = set()
    uniq = []
    for r in receipts:
        qt = _normalize_quote_text(r.get("quote") or r.get("excerpt") or "")
        if not qt:
            continue
        key = qt.lower()
        if key in seen:
            continue
        seen.add(key)
        uniq.append(r)

    return uniq


def _pick_striking_receipts(all_receipts: List[Dict[str, Any]], n: int = 5) -> List[Dict[str, Any]]:
    scored = []
    used_channels = set()

    for r in all_receipts:
        qt = _normalize_quote_text(r.get("quote") or r.get("excerpt") or "")
        if not qt:
            continue
        score = _quote_score(qt)
        if score <= 0.0:
            continue
        scored.append((score, r))

    scored.sort(key=lambda x: x[0], reverse=True)

    picked: List[Dict[str, Any]] = []
    for score, r in scored:
        ch = (r.get("channel_name") or r.get("channel") or "").strip().lower()
        # prefer channel diversity early
        if ch and ch in used_channels and len(picked) < n - 1:
            continue
        picked.append(r)
        if ch:
            used_channels.add(ch)
        if len(picked) >= n:
            break

    return picked


# ----------------------------
# Narrative + Elias voice
# ----------------------------

def _build_narrative_lead(themes: List[Dict[str, Any]], books: List[Dict[str, Any]], snapshot: Dict[str, Any]) -> str:
    curr = _extract_current_period(snapshot)
    avg_axes = _safe_dict(curr.get("avg_axes"))

    drift = _extract_drift_rate(snapshot)
    delta = drift.get("delta", 0.0)

    theme_line = ""
    if themes:
        t0 = themes[0]
        theme_line = f"The loudest convergence across the network was **{_title_case(t0.get('theme',''))}**."
        if len(themes) >= 2:
            t1 = themes[1]
            theme_line += f" Close behind: **{_title_case(t1.get('theme',''))}**."

    book_line = ""
    if books:
        b0 = books[0]
        book_line = (
            f"Scripture gravity centered most on **{_title_case(b0.get('book',''))}** "
            f"({b0.get('total_references', 0)} references across {b0.get('sermon_count', 0)} sermons)."
        )

    axis_line = ""
    if avg_axes:
        try:
            hope = float(avg_axes.get("hope_vs_fear", 0.0))
            if hope >= 0.35:
                axis_line = "Tone leaned notably hopeful—more lift than threat."
            elif hope <= -0.20:
                axis_line = "Tone leaned heavier—more warning than lift."
            else:
                axis_line = "Tone held fairly balanced—neither panic nor triumphalism dominated."
        except Exception:
            axis_line = ""

    drift_line = ""
    try:
        d = float(delta)
        if abs(d) >= 7.5:
            if d > 0:
                drift_line = f"Drift increased versus the prior period (Δ {_fmt_num(d)} points): more directional movement, more shifting emphasis."
            else:
                drift_line = f"Drift decreased versus the prior period (Δ {_fmt_num(d)} points): the network felt steadier, fewer sharp pivots."
        else:
            drift_line = "Drift stayed relatively steady versus the prior period—no major macro whiplash."
    except Exception:
        drift_line = ""

    p1_parts = [x for x in [theme_line, book_line, axis_line] if x]
    p2_parts = [x for x in [drift_line] if x]

    p1 = " ".join(p1_parts).strip()
    p2 = (
        "What matters isn’t the metric—it’s what the metrics reveal about formation. "
        + " ".join(p2_parts)
        + " Use this as a listening aid: to clarify your next emphasis, and to spot drift early before it becomes culture."
    ).strip()

    return p1 + "\n\n" + p2


def _ensure_elias_observations(
    themes: List[Dict[str, Any]],
    books: List[Dict[str, Any]],
    snapshot: Dict[str, Any],
    observations: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    normalized = []
    for ob in observations:
        statement = (ob.get("statement") or ob.get("text") or ob.get("observation") or "").strip()
        qb = ob.get("quote_bank") if isinstance(ob.get("quote_bank"), list) else []
        if statement:
            normalized.append({"speaker": "Elias", "statement": statement, "quote_bank": qb})

    if len(normalized) >= 3:
        return normalized[:6]

    curr = _extract_current_period(snapshot)
    avg_axes = _safe_dict(curr.get("avg_axes"))
    drift = _extract_drift_rate(snapshot)

    fallbacks: List[Dict[str, Any]] = []

    if themes:
        t0 = themes[0]
        fallbacks.append({
            "speaker": "Elias",
            "statement": f"Signal: **{_title_case(t0.get('theme',''))}** kept showing up across the network. Meaning: that’s where people are being formed—either toward clarity or toward cliché. Action: tighten the center.",
            "quote_bank": []
        })

    if books:
        b0 = books[0]
        fallbacks.append({
            "speaker": "Elias",
            "statement": f"Signal: **{_title_case(b0.get('book',''))}** carried the most weight this period. Meaning: our shared imagination is being shaped there. Action: preach it in a way that reveals Christ, not just supports a point.",
            "quote_bank": []
        })

    if avg_axes:
        try:
            hope = float(avg_axes.get("hope_vs_fear", 0.0))
            if hope >= 0.35:
                line = "Signal: tone leaned hopeful. Meaning: people are hungry for lift that’s rooted in truth. Action: keep hope tethered to Scripture, not vibes."
            elif hope <= -0.20:
                line = "Signal: tone leaned heavy. Meaning: people may be carrying fear or fatigue. Action: don’t soften truth—pair it with gospel oxygen."
            else:
                line = "Signal: tone stayed balanced. Meaning: no single emotional register dominated. Action: use that stability to deepen doctrine without losing warmth."
            fallbacks.append({"speaker": "Elias", "statement": line, "quote_bank": []})
        except Exception:
            pass

    try:
        d = float(drift.get("delta", 0.0))
        if abs(d) >= 7.5:
            fallbacks.append({
                "speaker": "Elias",
                "statement": f"Signal: drift moved meaningfully (Δ {_fmt_num(d)}). Meaning: the network is shifting emphasis. Action: name the shift out loud and decide whether to reinforce or correct it.",
                "quote_bank": []
            })
    except Exception:
        pass

    combined = normalized + fallbacks
    if len(combined) < 3:
        combined.append({
            "speaker": "Elias",
            "statement": "Signal: the network is speaking. Action: listen carefully before you respond.",
            "quote_bank": []
        })
    return combined[:6]


# ----------------------------
# Markdown rendering
# ----------------------------

def _render_theme_section(themes: List[Dict[str, Any]]) -> str:
    if not themes:
        return ""
    lines = ["### Theme convergence"]
    for t in themes[:6]:
        name = _title_case(str(t.get("theme", "")))
        sermons = t.get("sermon_count", 0)
        avg = t.get("avg_density", None)
        total = t.get("total_density", None)

        bits = [f"**{name}**"]
        if sermons:
            bits.append(f"{_fmt_num(sermons)} sermons")
        if avg is not None:
            bits.append(f"avg density {_fmt_num(avg)}")
        if total is not None:
            bits.append(f"total {_fmt_num(total)}")

        lines.append("- " + " — ".join(bits))
    return "\n".join(lines)


def _render_books_section(books: List[Dict[str, Any]]) -> str:
    if not books:
        return ""
    lines = ["### Scripture focus"]
    for b in books[:7]:
        book = _title_case(str(b.get("book", "")))
        refs = b.get("total_references", 0)
        sermons = b.get("sermon_count", 0)
        avg = b.get("avg_refs_per_sermon", None)

        line = f"- **{book}** — {_fmt_num(refs)} references across {_fmt_num(sermons)} sermons"
        if avg is not None:
            line += f" (avg {_fmt_num(avg)}/sermon)"
        lines.append(line)
    return "\n".join(lines)


def _render_striking_receipts(receipts: List[Dict[str, Any]]) -> str:
    if not receipts:
        return ""
    lines = ["## Striking receipts (what people actually said)"]
    for r in receipts:
        block = _format_receipt_block(r)
        if block:
            lines.append(block)
            lines.append("")
    return "\n".join(lines).rstrip()


def _render_elias_observations(observations: List[Dict[str, Any]]) -> str:
    lines = ["## Elias (signal → meaning → action)"]
    idx = 1
    for ob in observations[:6]:
        statement = (ob.get("statement") or "").strip()
        if not statement:
            continue
        lines.append(f"**{idx}.** {statement}")

        qb = ob.get("quote_bank") if isinstance(ob.get("quote_bank"), list) else []
        # Keep it tight: 0–1 per observation (we already surfaced 3–5 above)
        if qb:
            for r in qb:
                if isinstance(r, dict):
                    qt = _normalize_quote_text(r.get("quote") or r.get("excerpt") or "")
                    if not qt or _is_garbled_asr(qt):
                        continue
                    block = _format_receipt_block(r)
                    if block:
                        lines.append("")
                        lines.append(block)
                        break
        lines.append("")
        idx += 1
    return "\n".join(lines).rstrip()


def _render_resonant(resonant: List[Dict[str, Any]]) -> str:
    if not resonant:
        return ""
    lines = ["## Resonant sermons (worth re-listening to)"]
    for it in resonant[:7]:
        ch = it.get("channel_name", "")
        title = it.get("title", "")
        reason = it.get("reason", "")
        lines.append(f"- **{ch}** — {title}  \n  *{reason}*")
    return "\n".join(lines)


def _render_outliers(outliers: List[Dict[str, Any]]) -> str:
    if not outliers:
        return ""
    lines = ["## Outliers (strong shifts / anomalies)"]
    for it in outliers[:5]:
        ch = it.get("channel_name", "")
        title = it.get("title", "")
        lvl = it.get("drift_level", "")
        mag = it.get("drift_magnitude", "")
        lines.append(f"- **{ch}** — {title}  \n  *{lvl} (magnitude {_fmt_num(mag)})*")
    return "\n".join(lines)


def build_substack_markdown(agenda: Dict[str, Any]) -> Tuple[str, str]:
    title = _pick_title(agenda)
    snapshot = _extract_snapshot(agenda)
    themes = _extract_themes(agenda)
    books = _extract_books(agenda)
    observations = _extract_observations(agenda)
    resonant = _extract_resonant(agenda)
    outliers = _extract_outliers(agenda)

    narrative = _build_narrative_lead(themes, books, snapshot)
    observations_final = _ensure_elias_observations(themes, books, snapshot, observations)

    all_receipts = _collect_all_receipts(observations_final, resonant)
    striking = _pick_striking_receipts(all_receipts, n=5)

    generated = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

    parts: List[str] = []
    parts.append(f"# {title}")
    parts.append(f"*Generated: {generated}*")
    parts.append("")
    parts.append(narrative)
    parts.append("")

    theme_sec = _render_theme_section(themes)
    if theme_sec:
        parts.append(theme_sec)
        parts.append("")

    book_sec = _render_books_section(books)
    if book_sec:
        parts.append(book_sec)
        parts.append("")

    striking_sec = _render_striking_receipts(striking)
    if striking_sec:
        parts.append(striking_sec)
        parts.append("")

    parts.append(_render_elias_observations(observations_final))
    parts.append("")

    res_sec = _render_resonant(resonant)
    if res_sec:
        parts.append(res_sec)
        parts.append("")

    out_sec = _render_outliers(outliers)
    if out_sec:
        parts.append(out_sec)
        parts.append("")

    parts.append("## What to do with this (next 7 days)")
    parts.append(
        "- Choose one convergent theme and preach **one clear corrective or reinforcement**.\n"
        "- Pick one striking receipt and ask: *Is this faithful emphasis? If not, what’s the correction?*\n"
        "- If drift is moving, name the shift to your team and decide: **reinforce or rebalance**."
    )
    parts.append("")

    body = "\n".join(parts).strip() + "\n"
    return title, body


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate Substack-ready Markdown from climate_agenda JSON.")
    ap.add_argument("--agenda_json", required=True, help="Path to climate_agenda output JSON file.")
    ap.add_argument("--out_dir", default="out/substack", help="Directory for output .md drafts.")
    ap.add_argument("--prefix", default="", help="Optional filename prefix.")
    args = ap.parse_args()

    agenda_path = Path(args.agenda_json)
    if not agenda_path.exists():
        raise SystemExit(f"agenda_json not found: {agenda_path}")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    with agenda_path.open("r", encoding="utf-8") as f:
        agenda = json.load(f)

    title, md = build_substack_markdown(agenda)
    stamp = datetime.utcnow().strftime("%Y-%m-%d")
    out_path = out_dir / f"{args.prefix}{stamp}_{_slugify(title)}.md"
    out_path.write_text(md, encoding="utf-8")
    print(str(out_path))


if __name__ == "__main__":
    main()
