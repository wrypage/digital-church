#!/usr/bin/env python3
"""
Render climate_agenda.json into a human-readable Markdown report (Substack-ready).

Usage:
  python -m engine.tools.render_climate_agenda_report --in out/climate_agenda.json --out out/climate_agenda.md
"""

from __future__ import annotations
import argparse
import json
from pathlib import Path
from datetime import datetime


def _fmt_dt(s: str) -> str:
    if not s:
        return ""
    try:
        # Accept "2026-02-20T16:31:45.190491Z" or "2026-02-18 10:00:00"
        if "T" in s:
            s2 = s.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s2)
        else:
            dt = datetime.fromisoformat(s)
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    except Exception:
        return s


def _title_case_theme(theme: str) -> str:
    return (theme or "").replace("_", " ").strip().title()


def _md_quote_block(q: dict) -> str:
    ch = q.get("channel_name") or ""
    title = q.get("title") or ""
    dt = q.get("published_at") or ""
    ex = (q.get("excerpt") or "").strip()

    lines = []
    lines.append(f"> {ex}")
    meta = " — ".join([p for p in [title, ch] if p])
    if meta:
        lines.append(f"> \n> *“{meta}”*")
    if dt:
        lines.append(f"> \n> *{_fmt_dt(dt)}*")
    return "\n".join(lines)


def render_md(data: dict) -> str:
    snap = data.get("climate_snapshot", {}).get("climate_snapshot", {}) or {}
    curr = snap.get("current", {}) or {}
    prev = snap.get("previous", {}) or {}
    deltas = snap.get("deltas", {}) or {}

    themes = data.get("theme_convergence", []) or []
    books = data.get("scripture_focus", []) or []
    observations = data.get("observations", []) or []
    resonant = data.get("resonant_sermons", []) or []
    outliers = data.get("outliers", []) or []

    generated_at = data.get("metadata", {}).get("generated_at") or snap.get("generated_at") or ""
    days = data.get("metadata", {}).get("days") or snap.get("period_days") or ""

    # Headline: top theme + top book
    top_theme = _title_case_theme(themes[0]["theme"]) if themes else "Signals"
    top_book = (books[0]["book"] or "").title() if books else "Scripture"

    lines = []
    lines.append(f"# This Week’s Signals: {top_theme} + {top_book}")
    if generated_at:
        lines.append(f"*Generated: {_fmt_dt(generated_at)}*")
    if days:
        lines.append(f"*Window: last {days} days*")
    lines.append("")

    # Elias preface (human voice)
    preface = data.get("elias_preface") or ""
    if preface:
        lines.append(preface)
        lines.append("")

    # Snapshot highlights
    lines.append("## Climate snapshot (macro)")
    lines.append(f"- Sermons analyzed (current): **{curr.get('count', 0)}**")
    lines.append(f"- Avg theological density: **{curr.get('avg_density', 0):.2f}** (Δ {deltas.get('density', 0):+.2f})")
    drift = curr.get("drift_distribution", {}) or {}
    lines.append(f"- Drift: stable **{drift.get('stable', 0)}**, moderate **{drift.get('moderate_shift', 0)}**, strong **{drift.get('strong_shift', 0)}**")
    lines.append("")

    # Theme convergence
    lines.append("## Theme convergence")
    for t in themes:
        lines.append(
            f"- **{_title_case_theme(t.get('theme',''))}** — {t.get('sermon_count',0)} sermons — avg {t.get('avg_density',0)} — total {t.get('total_density',0)}"
        )
    lines.append("")

    # Scripture focus
    lines.append("## Scripture focus")
    for b in books:
        lines.append(
            f"- **{(b.get('book') or '').title()}** — {b.get('total_references',0)} refs across {b.get('sermon_count',0)} sermons (avg {b.get('avg_refs_per_sermon',0)}/sermon)"
        )
    lines.append("")

    # Elias observations (with receipts)
    lines.append("## Elias (signal → meaning)")
    for i, obs in enumerate(observations, start=1):
        statement = (obs.get("statement") or "").strip()
        mode = (obs.get("mode") or "").strip()
        lines.append(f"**{i}.** ({mode}) {statement}")
        qb = obs.get("quote_bank") or []
        if qb:
            lines.append("")
            lines.append("**Receipts:**")
            for q in qb:
                lines.append(_md_quote_block(q))
                lines.append("")
        else:
            lines.append("")
    # Closing
    closing = data.get("elias_closing_line") or ""
    if closing:
        lines.append(f"**Closing:** {closing}")
        lines.append("")

    # Resonant sermons
    lines.append("## Resonant sermons (worth re-listening)")
    for s in resonant:
        title = s.get("title") or ""
        ch = s.get("channel_name") or ""
        reason = s.get("reason") or ""
        dt = s.get("published_at") or ""
        lines.append(f"### {title} — {ch}")
        if dt:
            lines.append(f"*{_fmt_dt(dt)}*")
        if reason:
            lines.append(f"- {reason}")
        lines.append(f"- Theological density: **{s.get('theological_density',0):.2f}**")
        lines.append(f"- Theme density: **{s.get('category_density',0):.2f}**")
        qb = s.get("quote_bank") or []
        if qb:
            lines.append("")
            lines.append("**Receipts from this sermon:**")
            for q in qb[:3]:
                lines.append(_md_quote_block(q))
                lines.append("")
        else:
            lines.append("")
            lines.append("_No usable receipts survived the filter for this sermon yet._")
            lines.append("")
    lines.append("")

    # Outliers
    if outliers:
        lines.append("## Outliers (strong shifts)")
        for o in outliers:
            lines.append(
                f"- **{o.get('channel_name','')}** — {o.get('title','')} "
                f"(*{_fmt_dt(o.get('published_at',''))}*) — {o.get('drift_level','')} (mag {o.get('drift_magnitude',0)})"
            )
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True, help="Input JSON path (e.g., out/climate_agenda.json)")
    ap.add_argument("--out", dest="out_path", required=True, help="Output Markdown path (e.g., out/climate_agenda.md)")
    args = ap.parse_args()

    in_path = Path(args.in_path)
    out_path = Path(args.out_path)

    data = json.loads(in_path.read_text(encoding="utf-8"))
    md = render_md(data)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(md, encoding="utf-8")

    print(f"Wrote: {out_path}")


if __name__ == "__main__":
    main()