# engine/elias_closer.py

from __future__ import annotations
from datetime import datetime
from typing import Dict, List, Any
import hashlib


def _stable_pick(options: List[str], seed: str) -> str:
    h = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    i = int(h[:8], 16) % len(options)
    return options[i]


def choose_closer_style(generated_at_iso: str) -> str:
    """
    Rotation:
      60% tension_question
      20% framing_contrast
      10% quiet_warning
      10% pattern_statement
    Deterministic by week so output doesn’t jitter run-to-run.
    """
    dt = datetime.fromisoformat(generated_at_iso.replace("Z", ""))
    year, week, _ = dt.isocalendar()
    seed = f"{year}-W{week}"

    # weighted wheel
    wheel = (
        ["tension_question"] * 6 +
        ["framing_contrast"] * 2 +
        ["quiet_warning"] * 1 +
        ["pattern_statement"] * 1
    )
    return _stable_pick(wheel, seed)


def render_closer(style: str, context: Dict[str, Any]) -> str:
    """
    context can include:
      - top_book (str)
      - top_theme (str)
      - tone_phrase (str)
      - tension (str)
    """
    top_book = context.get("top_book", "John")
    top_theme = context.get("top_theme", "Scripture")
    tension = context.get("tension", "Are we being formed in clarity—or in fear?")
    tone_phrase = context.get("tone_phrase", "There’s a sobriety in the room.")

    if style == "tension_question":
        return f"{tone_phrase} {tension}"

    if style == "framing_contrast":
        return f"{top_theme} is everywhere right now. But quoting Scripture isn’t the same thing as being formed by it."

    if style == "quiet_warning":
        return "Conviction without assurance has a predictable outcome in the human heart."

    if style == "pattern_statement":
        return f"When {top_book} dominates our shared attention, identity language rises with it—sometimes beautifully, sometimes cheaply. The difference is whether Christ is revealed or merely referenced."

    # fallback
    return f"{tone_phrase} {tension}"