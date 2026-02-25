#!/usr/bin/env python3
"""
engine/elias_writer.py

Elias:
- Observes
- Interprets
- Grounds in real sermon language
- Speaks as a Christian seeking God ("we")
- Uses paraphrased claims + optional receipts
"""

from __future__ import annotations
import sqlite3
from typing import Any, Dict, List

from engine.quote_bank import get_quotes
from engine.paraphrase import distill_claim, synthesize_network_claim


# ------------------------------
# Helpers
# ------------------------------

def _fmt_percent(x: float) -> str:
    return f"{round(x, 2)}%"


def _axis_summary(axes: Dict[str, float]) -> str:
    parts = []
    for axis, val in axes.items():
        direction = "leaning toward"
        if val < 0:
            direction = "leaning away from"
        parts.append(f"{axis.replace('_', ' ')} {direction} center")
    return ", ".join(parts)


# ------------------------------
# Core Writer
# ------------------------------

def write_elias_section(
    conn: sqlite3.Connection,
    climate_snapshot: Dict[str, Any],
    theme_convergence: List[Dict[str, Any]],
    scripture_focus: List[Dict[str, Any]],
    days: int = 30,
    quotes_per_section: int = 3
) -> List[str]:

    lines: List[str] = []

    current = climate_snapshot["current"]
    deltas = climate_snapshot.get("deltas", {})

    # ----------------------------------
    # Opening Tone
    # ----------------------------------

    lines.append("## Elias")
    lines.append("")
    lines.append(
        "I sat with these sermons the way I would sit in our own church—"
        "Bible open, asking what is shaping us."
    )
    lines.append("")

    # ----------------------------------
    # Theme Convergence
    # ----------------------------------

    if theme_convergence:
        top_theme = theme_convergence[0]
        lines.append(
            f"We leaned heavily into **{top_theme['theme'].replace('_',' ')}** "
            f"this month—{top_theme['sermon_count']} sermons carrying that weight."
        )
        lines.append("")

        quotes = get_quotes(
            conn,
            days=days,
            category=top_theme["theme"],
            n=quotes_per_section,
            distinct_channels=True
        )

        claims = []
        for q in quotes:
            claim = distill_claim(q["excerpt"])
            if claim:
                claims.append(claim)

        network_claim = synthesize_network_claim(claims)
        if network_claim:
            lines.append(network_claim)
            lines.append("")

        # Optional sharp receipt
        if quotes:
            top = sorted(
                quotes,
                key=lambda x: x.get("intelligence_score", 0),
                reverse=True
            )[0]

            lines.append("> " + top["excerpt"])
            lines.append(f"*{top['title']} — {top['channel_name']}*")
            lines.append("")

    # ----------------------------------
    # Scripture Gravity
    # ----------------------------------

    if scripture_focus:
        top_book = scripture_focus[0]
        lines.append(
            f"We kept returning to **{top_book['book'].title()}**—"
            f"{top_book['total_references']} references across "
            f"{top_book['sermon_count']} sermons."
        )
        lines.append(
            "That tells us something about where our imagination is resting."
        )
        lines.append("")

    # ----------------------------------
    # Axes Movement
    # ----------------------------------

    axes = current.get("avg_axes", {})
    if axes:
        lines.append(
            "Across the network we are "
            + _axis_summary(axes)
            + "."
        )
        lines.append("")

    # ----------------------------------
    # Drift
    # ----------------------------------

    drift_rate = climate_snapshot.get("drift_rate", {}).get("current")
    if drift_rate is not None:
        lines.append(
            f"Drift sat at {_fmt_percent(drift_rate)}. "
            "Not a collapse. Not a revival. Just movement."
        )
        lines.append("")

    # ----------------------------------
    # Closing Reflection
    # ----------------------------------

    lines.append(
        "When we preach conviction without assurance, "
        "we quietly teach people to strive without rest."
    )
    lines.append("")
    lines.append(
        "If John is shaping us, may it not just increase our references—"
        "may it deepen our love for Christ Himself."
    )
    lines.append("")

    return lines