#!/usr/bin/env python3
"""
engine/paraphrase.py

Claim distillation layer for Elias.

Turns high-scoring excerpts into:
- Clean theological claims
- Network-level synthesis
- Pastoral tone
"""

import re
from typing import List


FILLER = [
    "welcome to",
    "thanks for joining",
    "let's dive",
    "turn with me",
    "today we're going",
    "as you're turning",
]

INTRO_PHRASES = [
    "i want to encourage you",
    "i remember when",
    "let me just",
    "we're going to",
]

WEAK_PHRASES = [
    "kind of",
    "sort of",
    "you know",
    "no doubt",
]


def _clean_text(text: str) -> str:
    text = re.sub(r"\s+", " ", text.strip())
    lower = text.lower()

    for phrase in FILLER + INTRO_PHRASES:
        lower = lower.replace(phrase, "")

    for weak in WEAK_PHRASES:
        lower = lower.replace(weak, "")

    return lower.strip().capitalize()


def _compress(text: str, max_words: int = 30) -> str:
    words = text.split()
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words]) + "…"


def distill_claim(excerpt: str) -> str:
    """
    Converts excerpt into a clean theological claim.
    """
    cleaned = _clean_text(excerpt)

    # Remove trailing fragments
    cleaned = re.sub(r"\.\.\.$", "", cleaned)
    cleaned = cleaned.strip()

    # Prefer full sentences
    if "." in cleaned:
        cleaned = cleaned.split(".")[0]

    cleaned = _compress(cleaned)

    # Ensure it reads like a claim
    if not cleaned.endswith("."):
        cleaned += "."

    return cleaned


def synthesize_network_claim(claims: List[str]) -> str:
    """
    Converts multiple distilled claims into a network-level observation.
    """
    if not claims:
        return ""

    if len(claims) == 1:
        return f"We keep hearing this: {claims[0]}"

    # Combine themes gently
    combined = claims[:2]
    return (
        "Across different pulpits, a similar burden surfaced: "
        + " ".join(combined)
    )