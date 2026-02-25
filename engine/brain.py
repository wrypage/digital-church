# engine/brain.py
import argparse
import json
import math
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from .config import DATABASE_PATH, load_theology_config

# -----------------------------
# Settings
# -----------------------------
DEFAULT_BASELINE_WINDOW = 4
MAX_EVIDENCE_PER_AXIS = 4
MAX_EVIDENCE_PER_CATEGORY = 3
SNIPPET_WORD_WINDOW = 28  # words around keyword match
MIN_WORD_COUNT = 100      # also enforced by config if present

# Intent extraction hygiene settings
INTENT_SKIP_EDGE_RATIO = 0.04     # skip early/late transcript edge (intros/outros)
INTENT_MIN_SENT_WORDS = 9         # avoid micro-sentences becoming CTAs
INTENT_MAX_SENTENCES_SCAN = 260   # keep it light

# Axis scoring calibration (prevents ceiling saturation)
AXIS_INERTIA_K = 2.0              # inertia dampener: prevents ±1.0 when one side is zero
AXIS_MIN_ACTIVATION = 1.0         # minimum total density to produce non-zero axis score


# -----------------------------
# Boilerplate / bumper detection (KEY: stop “Marked By Grace…”, URLs, etc.)
# -----------------------------
BOILERPLATE_PATTERNS = [
    # Show bumpers / podcast meta
    r"\bnow (?:let\'?s|lets) dive into (?:today\'?s|this) (?:teaching|message)\b",
    r"\bnow (?:let\'?s|lets) get into (?:today\'?s|this) (?:teaching|message)\b",
    r"\bwelcome to\b",
    r"\bthanks for (?:joining|tuning in)\b",
    r"\bwe\'?re glad you\'?re here\b",
    r"\byou\'?re listening to\b",
    r"\byou are listening to\b",
    r"\bthis is\b.*\bpodcast\b",
    r"\bsubscribe\b",
    r"\blike and subscribe\b",
    r"\bturn on notifications\b",
    r"\bsmash that like\b",

    # Contact / question scripts
    r"\bif you have a question\b",
    r"\bsend (?:that|your question)\b",
    r"\bsend your question\b",
    r"\bemail us\b",
    r"\bcall the office\b",
    r"\bcontact us\b",

    # URLs / emails / domains / spelling-out web addresses
    r"\bhttps?://\S+\b",
    r"\bwww\.\S+\b",
    r"\b\S+\.(?:com|org|net|io|co|us|tv)\b",
    r"\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b",
    r"\bdot com\b",
    r"\bwith an x\b",

    # Fundraising / support scripts
    r"\bsupport this ministry\b",
    r"\bpartner with\b",
    r"\bdonate\b",
    r"\bgive (?:today|now)\b",
    r"\bto thank you for your support\b",
    r"\bwe\'?ll send you\b",
    r"\bevery day, the generosity of friends like you\b",
    r"\byour gift\b.*\bhelps\b",
    r"\btext\s+\w+\s+to\s+\d+\b",
    r"\bthank you for (?:your|the) support\b",

    # Announcements
    r"\bannouncements\b",
    r"\bservice times\b",
    r"\bwe have (?:prayer|bible study)\b",
    r"\bevery (?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b.*\b(prayer|service|bible study|group)\b",

    # Common brand taglines (your offender: Marked By Grace / FBCJacks)
    r"\bmarked by grace\b",
    r"\bfbcjacks\b",
    r"\bfbcjax\b",
]

_BOILERPLATE_RE = re.compile("|".join(f"(?:{p})" for p in BOILERPLATE_PATTERNS), flags=re.IGNORECASE)
_EMAIL_RE = re.compile(r"[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}", flags=re.IGNORECASE)
_URL_RE = re.compile(r"(https?://\S+|www\.\S+|\b\S+\.(com|org|net|io|co|us|tv)\b)", flags=re.IGNORECASE)


def is_boilerplate(text: str) -> bool:
    if not text:
        return False
    t = " ".join(text.strip().split())
    if not t:
        return False
    # structural fast rejects
    if _EMAIL_RE.search(t) or _URL_RE.search(t):
        return True
    return bool(_BOILERPLATE_RE.search(t))


# -----------------------------
# Utilities
# -----------------------------
def _connect(db_path: str = DATABASE_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def _normalize_text(text: str) -> str:
    if not text:
        return ""
    t = text.lower()
    # keep digits and colon for scripture refs; remove most punctuation
    t = re.sub(r"[^\w\s:\-]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _safe_json_load(s: Optional[str], default):
    if not s:
        return default
    try:
        return json.loads(s)
    except Exception:
        return default


# -----------------------------
# Summary-first (clean summary_text stored in transcripts)
# -----------------------------
SUMMARY_MODEL = os.environ.get("DP_SUMMARY_MODEL", "gpt-4o-mini")
SUMMARY_MAX_TOKENS = int(os.environ.get("DP_SUMMARY_MAX_TOKENS", "900"))
SUMMARY_MIN_WORDS = int(os.environ.get("DP_SUMMARY_MIN_WORDS", "120"))


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table});").fetchall()
    for r in rows:
        if (r["name"] or "").lower() == column.lower():
            return True
    return False


def ensure_summary_column(conn: sqlite3.Connection) -> None:
    """Add transcripts.summary_text if missing (additive, not a redesign)."""
    if _column_exists(conn, "transcripts", "summary_text"):
        return
    conn.execute("ALTER TABLE transcripts ADD COLUMN summary_text TEXT;")
    conn.commit()


def _word_count(text: str) -> int:
    if not text:
        return 0
    return len(re.findall(r"\b\w+\b", text))


def _truncate(s: str, n: int) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    return s[:n].rstrip() + "…"


SUMMARY_SYSTEM = """You are a sermon distillation engine.
Your job is to produce a CLEAN SUMMARY that removes noise and preserves the core burden.

Rules:
- Remove: bumpers, welcomes, announcements, giving promos, podcast/subscribe lines, greetings, stage directions.
- Remove: long scripture-reading blocks (you may mention the passage, but do not quote long sections).
- Remove: repetitive rhetorical loops, filler, crowd work.
- Keep: the sermon’s core thesis, main movements, main warnings, encouragements, and calls to action.
- Do NOT add your own commentary or evaluation.
- Do NOT preach.
- Do NOT moralize.
- Output: plain text (no JSON), clean paragraphs, 400–800 words.
"""


SUMMARY_USER_TEMPLATE = """Distill the sermon transcript below into a clean summary.

Metadata:
- Title: {title}
- Published: {published_at}

Transcript:
{transcript}
"""


def _get_openai_client():
    """Modern OpenAI client (v1+). Requires OPENAI_API_KEY."""
    from openai import OpenAI
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY not set. Required for summary generation.")
    return OpenAI(api_key=api_key)


def _generate_clean_summary(title: str, published_at: str, full_text: str) -> str:
    if not full_text or _word_count(full_text) < 200:
        return ""

    client = _get_openai_client()
    user_msg = SUMMARY_USER_TEMPLATE.format(
        title=_truncate(title, 140),
        published_at=_truncate(published_at, 40),
        transcript=full_text,
    )

    resp = client.chat.completions.create(
        model=SUMMARY_MODEL,
        messages=[
            {"role": "system", "content": SUMMARY_SYSTEM},
            {"role": "user", "content": user_msg},
        ],
        max_tokens=SUMMARY_MAX_TOKENS,
    )
    text = (resp.choices[0].message.content or "").strip()
    if _word_count(text) < SUMMARY_MIN_WORDS:
        return ""
    return text


def get_or_create_summary(
    conn: sqlite3.Connection,
    video_id: str,
    title: str,
    published_at: str,
    full_text: str,
) -> str:
    row = conn.execute(
        "SELECT summary_text FROM transcripts WHERE video_id = ? LIMIT 1;",
        (video_id,),
    ).fetchone()

    if row and (row["summary_text"] or "").strip():
        return (row["summary_text"] or "").strip()

    summary = _generate_clean_summary(title=title, published_at=published_at, full_text=full_text)
    if summary:
        conn.execute(
            "UPDATE transcripts SET summary_text = ? WHERE video_id = ?;",
            (summary, video_id),
        )
        conn.commit()
    return summary


def _mean(vals: List[float]) -> float:
    if not vals:
        return 0.0
    return sum(vals) / len(vals)


def _std(vals: List[float], mean: float) -> float:
    if len(vals) < 2:
        return 0.0
    var = sum((x - mean) ** 2 for x in vals) / (len(vals) - 1)
    return math.sqrt(var)


def _zscore(x: float, mu: float, sigma: float) -> float:
    MIN_STD = 0.05  # Prevent inflated z-scores in small datasets
    if sigma <= MIN_STD:
        return 0.0
    return (x - mu) / sigma


def _top_n_dict(d: Dict[str, float], n: int) -> List[Tuple[str, float]]:
    return sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:n]


# -----------------------------
# Brain config
# -----------------------------
@dataclass
class BrainConfig:
    categories: Dict[str, Dict]
    axes: Dict[str, Dict]
    normalization_method: str
    min_word_count: int


def load_brain_config() -> BrainConfig:
    cfg = load_theology_config()
    if not cfg:
        raise RuntimeError("Theology config could not be loaded. Check data/digital_pulpit_config.json")

    cats = cfg.get("theological_categories", {})
    axes = cfg.get("drift_axes", {})
    norm = cfg.get("density_normalization", {}) or {}

    method = norm.get("method", "per_1000_words")
    min_wc = int(norm.get("min_word_count", MIN_WORD_COUNT))

    return BrainConfig(
        categories=cats,
        axes=axes,
        normalization_method=method,
        min_word_count=min_wc,
    )


# -----------------------------
# Category + axis scoring
# -----------------------------
def score_categories(text: str, cfg: BrainConfig) -> Tuple[Dict[str, int], Dict[str, float]]:
    normalized = _normalize_text(text)
    words = normalized.split(" ")
    word_count = len(words)

    counts: Dict[str, int] = {}
    density: Dict[str, float] = {}

    if word_count <= 0:
        return {}, {}

    for cat, meta in cfg.categories.items():
        kws = meta.get("keywords", [])
        weight = float(meta.get("weight", 1.0))
        c = 0
        for kw in kws:
            kw_n = _normalize_text(kw)
            if not kw_n:
                continue
            if " " in kw_n:
                c += len(re.findall(re.escape(kw_n), normalized))
            else:
                c += len(re.findall(r"\b" + re.escape(kw_n) + r"\b", normalized))
        c = int(round(c * weight))
        counts[cat] = c

        if cfg.normalization_method == "per_1000_words":
            density[cat] = (c / word_count) * 1000.0
        else:
            density[cat] = float(c)

    return counts, density


def score_axes(category_density: Dict[str, float], cfg: BrainConfig) -> Dict[str, float]:
    """
    Score theological axes with inertia dampening to prevent ceiling saturation.

    Inertia dampener (K=2.0): prevents scores from hitting ±1.0 when one side is zero,
    preserving polarity and signal volume without biasing the axis.

    Minimum activation (3.0): below this total density, signal is too thin to score.
    """
    out: Dict[str, float] = {}
    for axis, meta in cfg.axes.items():
        pos = meta.get("positive")
        neg = meta.get("negative")
        pos_v = float(category_density.get(pos, 0.0))
        neg_v = float(category_density.get(neg, 0.0))
        total = pos_v + neg_v

        if total < AXIS_MIN_ACTIVATION:
            # Signal too weak - return neutral
            out[axis] = 0.0
        else:
            # Apply inertia dampening: (pos - neg) / (total + K)
            out[axis] = (pos_v - neg_v) / (total + AXIS_INERTIA_K)

    return out


def theological_density(category_density: Dict[str, float]) -> float:
    return float(sum(category_density.values()))


# -----------------------------
# Scripture extraction (basic, fast)
# -----------------------------
BOOK_PATTERNS = [
    ("genesis", [r"genesis", r"\bgen\b"]),
    ("exodus", [r"exodus", r"\bexo\b", r"\bex\b"]),
    ("deuteronomy", [r"deuteronomy", r"\bdeut\b", r"\bdeu\b"]),
    ("psalms", [r"psalms", r"\bpsalm\b", r"\bps\b"]),
    ("proverbs", [r"proverbs", r"\bprov\b", r"\bpr\b"]),
    ("isaiah", [r"isaiah", r"\bisa\b"]),
    ("jeremiah", [r"jeremiah", r"\bjer\b"]),
    ("matthew", [r"matthew", r"\bmatt\b", r"\bmt\b"]),
    ("mark", [r"\bmark\b", r"\bmk\b"]),
    ("luke", [r"\bluke\b", r"\blk\b"]),
    ("john", [r"\bjohn\b", r"\bjn\b"]),
    ("acts", [r"\bacts\b"]),
    ("romans", [r"\bromans\b", r"\brom\b"]),
    ("1 corinthians", [r"\b1\s*corinthians\b", r"\b1\s*cor\b", r"\bi\s*cor\b"]),
    ("2 corinthians", [r"\b2\s*corinthians\b", r"\b2\s*cor\b", r"\bii\s*cor\b"]),
    ("galatians", [r"\bgalatians\b", r"\bgal\b"]),
    ("ephesians", [r"\bephesians\b", r"\beph\b"]),
    ("philippians", [r"\bphilippians\b", r"\bphil\b"]),
    ("colossians", [r"\bcolossians\b", r"\bcol\b"]),
    ("1 thessalonians", [r"\b1\s*thessalonians\b", r"\b1\s*thess\b"]),
    ("2 thessalonians", [r"\b2\s*thessalonians\b", r"\b2\s*thess\b"]),
    ("1 timothy", [r"\b1\s*timothy\b", r"\b1\s*tim\b"]),
    ("2 timothy", [r"\b2\s*timothy\b", r"\b2\s*tim\b"]),
    ("hebrews", [r"\bhebrews\b", r"\bheb\b"]),
    ("james", [r"\bjames\b", r"\bjas\b"]),
    ("1 peter", [r"\b1\s*peter\b", r"\b1\s*pet\b"]),
    ("2 peter", [r"\b2\s*peter\b", r"\b2\s*pet\b"]),
    ("revelation", [r"\brevelation\b", r"\brev\b"]),
]

CHV_RE = r"(?:\s+(\d{1,3})(?::(\d{1,3})(?:\s*[-–]\s*(\d{1,3}))?)?)?"


def extract_scripture_refs(text: str) -> Dict[str, int]:
    if not text:
        return {}
    t = text.lower()
    counts: Dict[str, int] = {}
    for book, pats in BOOK_PATTERNS:
        combined = "(" + "|".join(pats) + ")" + CHV_RE
        matches = re.findall(combined, t, flags=re.IGNORECASE)
        if matches:
            counts[book] = len(matches)
    return counts


# -----------------------------
# Snippet extraction (evidence receipts)
# -----------------------------
def _word_window_excerpt(original_text: str, start_char: int, window_words: int) -> str:
    if not original_text:
        return ""
    char_window = 600
    lo = max(0, start_char - char_window)
    hi = min(len(original_text), start_char + char_window)
    chunk = original_text[lo:hi]
    chunk = re.sub(r"\s+", " ", chunk).strip()

    words = chunk.split(" ")
    if len(words) <= window_words * 2:
        return chunk

    mid = len(words) // 2
    lo_w = max(0, mid - window_words)
    hi_w = min(len(words), mid + window_words)
    return " ".join(words[lo_w:hi_w]).strip()


def find_keyword_snippets(full_text: str, keywords: List[str], max_snips: int) -> List[Tuple[str, str, int]]:
    if not full_text or not keywords or max_snips <= 0:
        return []

    text = full_text
    lower = text.lower()

    hits: List[Tuple[int, str]] = []
    for kw in keywords:
        kw_l = kw.lower().strip()
        if not kw_l:
            continue
        if " " in kw_l:
            pat = re.escape(kw_l)
        else:
            pat = r"\b" + re.escape(kw_l) + r"\b"
        m = re.search(pat, lower, flags=re.IGNORECASE)
        if m:
            hits.append((m.start(), kw))

    hits.sort(key=lambda x: x[0])
    out: List[Tuple[str, str, int]] = []
    seen_positions = set()

    for pos, kw in hits:
        if len(out) >= max_snips:
            break
        bucket = pos // 50
        if bucket in seen_positions:
            continue
        seen_positions.add(bucket)
        excerpt = _word_window_excerpt(text, pos, SNIPPET_WORD_WINDOW)
        out.append((kw, excerpt, pos))

    return out


# -----------------------------
# Signals (Brain v2.1)
# -----------------------------
def compute_signals(category_density: Dict[str, float]) -> Dict:
    """
    Compute Brain v2.1 signals from category densities.
    Returns dict with:
    - immanence_vs_transcendence: axis score [-1, 1]
    - mode_distribution: {prophetic, pastoral, didactic} proportions
    - trinity_distribution: {christ, father, spirit} proportions
    """
    eps = 1e-9

    imm = float(category_density.get("immanence", 0.0))
    trans = float(category_density.get("transcendence", 0.0))
    imm_vs_trans = (imm - trans) / (imm + trans + eps)

    prophetic = float(category_density.get("prophetic", 0.0))
    pastoral = float(category_density.get("pastoral", 0.0))
    didactic = float(category_density.get("didactic", 0.0))
    mode_total = prophetic + pastoral + didactic + eps
    mode_dist = {
        "prophetic": prophetic / mode_total,
        "pastoral": pastoral / mode_total,
        "didactic": didactic / mode_total,
    }

    christ = float(category_density.get("christ", 0.0))
    father = float(category_density.get("father", 0.0))
    spirit = float(category_density.get("spirit", 0.0))
    trinity_total = christ + father + spirit + eps
    trinity_dist = {
        "christ": christ / trinity_total,
        "father": father / trinity_total,
        "spirit": spirit / trinity_total,
    }

    return {
        "immanence_vs_transcendence": float(imm_vs_trans),
        "mode_distribution": mode_dist,
        "trinity_distribution": trinity_dist,
    }


# -----------------------------
# Intent vectors + tone profile (your current “good stuff”)
# -----------------------------
_SENT_SPLIT_RE = re.compile(r"(?<=[\.\!\?])\s+")


def _strip_edges(sentences: List[str], edge_ratio: float) -> List[str]:
    if not sentences:
        return sentences
    n = len(sentences)
    k = int(round(n * edge_ratio))
    if k <= 0:
        return sentences
    return sentences[k: max(k, n - k)]


def _clean_sentence(s: str) -> str:
    s = " ".join((s or "").strip().split())
    return s


def _sent_word_count(s: str) -> int:
    return len(re.findall(r"\b\w+\b", s or ""))


CTA_VERBS = [
    "repent", "turn", "pray", "seek", "trust", "believe", "follow", "obey",
    "forgive", "love", "serve", "give", "go", "share", "confess",
    "submit", "endure", "persevere", "stand", "fight", "worship",
    "read", "study", "remember", "hold fast", "be baptized",
]
WARN_MARKERS = [
    "beware", "watch out", "do not", "don't", "avoid", "warning", "caution",
    "lest", "otherwise", "if you", "or else",
]
ENCOURAGE_MARKERS = [
    "take heart", "be encouraged", "do not fear", "don't fear", "be strong",
    "god is with you", "the lord is with you", "you can", "there is hope",
    "he is faithful", "he will", "he can",
]


def extract_intent_vectors(text: str) -> Dict[str, Any]:
    """
    Extract intent vectors from text:
      - primary_burden + secondary burdens (simple clustering by repeated statements)
      - warnings
      - encouragements
      - calls_to_action

    Hygiene:
      - skip boilerplate
      - skip edges
      - skip tiny sentences
    """
    if not text:
        return {}

    # Split into sentences
    raw_sents = _SENT_SPLIT_RE.split(text.replace("\n", " ").strip())
    raw_sents = [_clean_sentence(s) for s in raw_sents if _clean_sentence(s)]
    raw_sents = [s for s in raw_sents if not is_boilerplate(s)]

    # Edge skip
    sents = _strip_edges(raw_sents, INTENT_SKIP_EDGE_RATIO)
    if INTENT_MAX_SENTENCES_SCAN and len(sents) > INTENT_MAX_SENTENCES_SCAN:
        sents = sents[:INTENT_MAX_SENTENCES_SCAN]

    # Filter sentence length
    sents = [s for s in sents if _sent_word_count(s) >= INTENT_MIN_SENT_WORDS]
    if not sents:
        return {}

    lowered = [s.lower() for s in sents]

    # Collect CTA, warnings, encouragements
    ctas = []
    warns = []
    encs = []

    for s, l in zip(sents, lowered):
        if any(v in l for v in CTA_VERBS):
            ctas.append(s)
        if any(m in l for m in WARN_MARKERS):
            warns.append(s)
        if any(m in l for m in ENCOURAGE_MARKERS):
            encs.append(s)

    # Primary burden heuristic: most "repeated-like" sentence by normalized n-gram overlap
    def _norm_key(s: str) -> str:
        t = _normalize_text(s)
        # drop common stopwords lightly
        t = re.sub(r"\b(the|a|an|and|or|but|to|of|in|on|for|with|as|at|by|from|that|this|it|is|are|was|were)\b", " ", t)
        t = re.sub(r"\s+", " ", t).strip()
        return t

    keys = [_norm_key(s) for s in sents]
    freq: Dict[str, int] = {}
    for k in keys:
        if not k:
            continue
        freq[k] = freq.get(k, 0) + 1

    ranked = sorted(freq.items(), key=lambda kv: kv[1], reverse=True)
    primary_key = ranked[0][0] if ranked else ""
    primary_count = ranked[0][1] if ranked else 0

    def _evidence_for_key(k: str) -> List[Dict[str, Any]]:
        ev = []
        for s, kk in zip(sents, keys):
            if kk == k:
                ev.append({"excerpt": s})
            if len(ev) >= 2:
                break
        return ev

    primary_burden = {}
    if primary_key:
        primary_burden = {
            "label": "primary_burden",
            "summary": primary_key,
            "confidence": min(0.95, 0.45 + 0.10 * primary_count),
            "evidence": _evidence_for_key(primary_key),
        }

    secondary = []
    for k, c in ranked[1:4]:
        if not k:
            continue
        secondary.append({
            "label": "secondary_burden",
            "summary": k,
            "confidence": min(0.9, 0.35 + 0.08 * c),
            "evidence": _evidence_for_key(k),
        })

    def _pack_list(rows: List[str], field: str) -> List[Dict[str, Any]]:
        out = []
        seen = set()
        for s in rows:
            k = _norm_key(s)
            if not k or k in seen:
                continue
            seen.add(k)
            out.append({
                field: s,
                "confidence": 0.6,
                "evidence": [{"excerpt": s}],
            })
            if len(out) >= 3:
                break
        return out

    return {
        "primary_burden": primary_burden,
        "secondary_burdens": secondary,
        "warnings": _pack_list(warns, "warning"),
        "encouragements": _pack_list(encs, "encouragement"),
        "calls_to_action": _pack_list(ctas, "action"),
        "assumed_concerns": [],
    }


def derive_tone_profile(axis_scores: Dict[str, float]) -> Dict[str, Any]:
    """
    Very lightweight tone derivation from axes.
    (Keeps your existing structure: climate_agenda consumes tone_profile->dominant_tone_tags)
    """
    tags = []
    hope = float(axis_scores.get("hope_vs_fear", 0.0))
    grace = float(axis_scores.get("grace_vs_effort", 0.0))
    script = float(axis_scores.get("scripture_vs_story", 0.0))
    doctr = float(axis_scores.get("doctrine_vs_experience", 0.0))

    if hope >= 0.25:
        tags.append("hopeful")
    elif hope <= -0.25:
        tags.append("warning-heavy")

    if grace >= 0.25:
        tags.append("grace-forward")
    elif grace <= -0.25:
        tags.append("effort-forward")

    if script >= 0.25:
        tags.append("text-anchored")
    elif script <= -0.25:
        tags.append("story-driven")

    if doctr >= 0.25:
        tags.append("doctrinal")
    elif doctr <= -0.25:
        tags.append("experiential")

    return {
        "dominant_tone_tags": tags[:5],
    }


# -----------------------------
# Baseline + drift
# -----------------------------
def compute_channel_baseline(
    conn: sqlite3.Connection,
    channel_id: str,
    window_n: int,
) -> Tuple[Dict[str, float], Dict[str, float], Dict[str, float], Dict[str, float]]:
    q = """
    SELECT br.grace_vs_effort, br.hope_vs_fear, br.doctrine_vs_experience, br.scripture_vs_story, br.raw_scores_json
    FROM brain_results br
    JOIN videos v ON br.video_id = v.video_id
    WHERE v.channel_id = ?
    ORDER BY v.published_at DESC
    LIMIT ?
    """
    rows = conn.execute(q, (channel_id, window_n)).fetchall()
    if not rows:
        return {}, {}, {}, {}

    axis_series: Dict[str, List[float]] = {
        "grace_vs_effort": [],
        "hope_vs_fear": [],
        "doctrine_vs_experience": [],
        "scripture_vs_story": [],
    }
    cat_series: Dict[str, List[float]] = {}

    for r in rows:
        for a in axis_series.keys():
            val = r[a]
            if val is None:
                continue
            axis_series[a].append(float(val))

        raw = _safe_json_load(r["raw_scores_json"], {})
        dens = raw.get("category_density", {}) or {}
        for cat, val in dens.items():
            try:
                fv = float(val)
            except Exception:
                continue
            cat_series.setdefault(cat, []).append(fv)

    axis_mean = {a: _mean(vals) for a, vals in axis_series.items() if vals}
    axis_std = {a: _std(vals, axis_mean[a]) for a, vals in axis_series.items() if vals}

    cat_mean = {c: _mean(vals) for c, vals in cat_series.items() if vals}
    cat_std = {c: _std(vals, cat_mean[c]) for c, vals in cat_series.items() if vals}

    return axis_mean, axis_std, cat_mean, cat_std


def classify_drift(zs: Dict[str, float]) -> str:
    if not zs:
        return "unknown"
    m = max(abs(v) for v in zs.values())
    if m >= 3.0:
        return "anomaly"
    if m >= 2.0:
        return "strong_shift"
    if m >= 1.25:
        return "moderate_shift"
    return "stable"


# -----------------------------
# Hooks
# -----------------------------
def build_hooks(
    axis_scores: Dict[str, float],
    axis_z: Dict[str, float],
    cat_density: Dict[str, float],
    cfg: BrainConfig
) -> Dict[str, Any]:
    hooks: Dict[str, Any] = {"tensions": [], "questions": []}

    dom_axis = None
    dom_val = 0.0
    for a, v in axis_scores.items():
        if abs(v) > abs(dom_val):
            dom_axis, dom_val = a, v

    direction = None
    if dom_axis:
        meta = cfg.axes.get(dom_axis, {})
        direction = meta.get("positive") if dom_val >= 0 else meta.get("negative")

    hooks["dominant_axis"] = dom_axis
    hooks["dominant_direction"] = direction
    hooks["dominant_strength"] = float(dom_val)

    for a, v in axis_scores.items():
        if abs(v) >= 0.45:
            meta = cfg.axes.get(a, {})
            pos = meta.get("positive")
            neg = meta.get("negative")
            favored = pos if v >= 0 else neg
            disfav = neg if v >= 0 else pos
            hooks["tensions"].append({
                "type": "imbalance",
                "axis": a,
                "favored": favored,
                "disfavored": disfav,
                "axis_score": float(v),
            })

    for a, z in axis_z.items():
        if abs(z) >= 2.0:
            hooks["tensions"].append({
                "type": "drift",
                "axis": a,
                "z": float(z),
                "note": "Axis deviated strongly from channel baseline",
            })

    if dom_axis:
        hooks["questions"].append("Is this emphasis a one-off sermon topic, or a directional shift for this channel?")
    if any(t.get("type") == "drift" for t in hooks["tensions"]):
        hooks["questions"].append("What changed recently (series, season, leadership, audience context) that could explain this deviation?")
    if any(t.get("type") == "imbalance" for t in hooks["tensions"]):
        hooks["questions"].append("Does this imbalance clarify the gospel, or risk distorting it by omission?")

    return hooks


# -----------------------------
# DB setup
# -----------------------------
def ensure_tables(conn: sqlite3.Connection) -> None:
    # Summary-first (additive column)
    ensure_summary_column(conn)

    conn.execute("""
    CREATE TABLE IF NOT EXISTS brain_evidence (
      evidence_id INTEGER PRIMARY KEY AUTOINCREMENT,
      video_id TEXT NOT NULL,
      channel_id TEXT,
      axis TEXT,
      category TEXT,
      keyword TEXT,
      excerpt TEXT NOT NULL,
      start_char INTEGER,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (video_id) REFERENCES videos(video_id)
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_brain_evidence_video_id ON brain_evidence(video_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_brain_evidence_channel_id ON brain_evidence(channel_id);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_brain_evidence_axis ON brain_evidence(axis);")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_brain_evidence_category ON brain_evidence(category);")

    conn.execute("""
    CREATE TABLE IF NOT EXISTS brain_baselines (
      channel_id TEXT NOT NULL,
      window_n INTEGER NOT NULL,
      mean_json TEXT NOT NULL,
      std_json TEXT NOT NULL,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (channel_id, window_n)
    );
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_brain_baselines_channel_id ON brain_baselines(channel_id);")
    conn.commit()


# -----------------------------
# Fetch work
# -----------------------------
def fetch_unanalyzed_transcripts(conn: sqlite3.Connection, limit: Optional[int] = None, recompute: bool = False, video_ids: Optional[List[str]] = None):
    q = """
    SELECT
        t.video_id,
        t.full_text,
        t.summary_text,
        t.word_count,
        t.segments_json,
        v.channel_id,
        v.title,
        v.published_at
    FROM transcripts t
    JOIN videos v ON t.video_id = v.video_id
    """

    params = []
    where_clauses = []

    # Base WHERE conditions
    where_clauses.append("t.full_text IS NOT NULL")
    where_clauses.append("t.word_count IS NOT NULL")

    # Filter by video_ids if provided
    if video_ids:
        placeholders = ",".join("?" * len(video_ids))
        where_clauses.append(f"t.video_id IN ({placeholders})")
        params.extend(video_ids)

    # Add recompute logic
    if not recompute:
        q += """
    LEFT JOIN brain_results br ON t.video_id = br.video_id
    """
        where_clauses.insert(0, "br.result_id IS NULL")

    q += " WHERE " + " AND ".join(where_clauses)
    q += " ORDER BY v.published_at DESC"

    if limit:
        q += " LIMIT ?"
        params.append(limit)

    return conn.execute(q, params).fetchall()


# -----------------------------
# Evidence writing
# -----------------------------
def write_evidence(
    conn: sqlite3.Connection,
    video_id: str,
    channel_id: str,
    axis: Optional[str],
    category: Optional[str],
    snippets: List[Tuple[str, str, int]],
) -> None:
    if not snippets:
        return
    for kw, excerpt, start_char in snippets:
        # Evidence hygiene: skip boilerplate/URLs/etc.
        if is_boilerplate(excerpt):
            continue

        conn.execute("""
        INSERT INTO brain_evidence (video_id, channel_id, axis, category, keyword, excerpt, start_char)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (video_id, channel_id, axis, category, kw, excerpt, int(start_char)))


def upsert_baseline(
    conn: sqlite3.Connection,
    channel_id: str,
    window_n: int,
    axis_mean: Dict[str, float],
    axis_std: Dict[str, float],
    cat_mean: Dict[str, float],
    cat_std: Dict[str, float],
) -> None:
    mean_json = json.dumps({"axes": axis_mean, "categories": cat_mean})
    std_json = json.dumps({"axes": axis_std, "categories": cat_std})
    conn.execute("""
    INSERT INTO brain_baselines (channel_id, window_n, mean_json, std_json, updated_at)
    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
    ON CONFLICT(channel_id, window_n)
    DO UPDATE SET mean_json=excluded.mean_json, std_json=excluded.std_json, updated_at=CURRENT_TIMESTAMP
    """, (channel_id, window_n, mean_json, std_json))


# -----------------------------
# Main analysis pipeline (SUMMARY-FIRST enforced)
# -----------------------------
def analyze_one(
    conn: sqlite3.Connection,
    cfg: BrainConfig,
    video_id: str,
    channel_id: str,
    title: str,
    published_at: str,
    full_text: str,
    summary_text: str,
    word_count: int,
    baseline_window: int = DEFAULT_BASELINE_WINDOW,
) -> None:
    if not full_text or (word_count or 0) < cfg.min_word_count:
        return

    # Ensure we have a stored clean summary (one-time, cached)
    if not (summary_text or "").strip():
        summary_text = get_or_create_summary(
            conn=conn,
            video_id=video_id,
            title=title,
            published_at=published_at,
            full_text=full_text,
        )

    # SUMMARY-FIRST enforcement: semantic scoring + intent runs on summary_text when available
    analysis_text = (summary_text or "").strip() or (full_text or "").strip()
    analysis_source = "summary_text" if (summary_text or "").strip() else "full_text"
    analysis_word_count = _word_count(analysis_text)

    # A) Prevent duplicate evidence on reruns
    conn.execute("DELETE FROM brain_evidence WHERE video_id = ?", (video_id,))

    # 1) category + axis scoring (SUMMARY-FIRST)
    cat_counts, cat_density = score_categories(analysis_text, cfg)
    axis_scores = score_axes(cat_density, cfg)
    theo_density = theological_density(cat_density)

    # 1b) Brain signals
    signals = compute_signals(cat_density)

    # 1c) intent vectors + tone profile — SUMMARY-FIRST
    intent_vectors = extract_intent_vectors(analysis_text)
    tone_profile = derive_tone_profile(axis_scores)

    # 2) scripture refs — FULL TEXT (keeps scripture anchoring strong)
    scripture_refs = extract_scripture_refs(full_text)

    # 3) baseline + zscores vs channel history
    axis_mean, axis_std, cat_mean, cat_std = compute_channel_baseline(conn, channel_id, baseline_window)

    baseline_ok = bool(axis_mean) and any((float(s) > 1e-9) for s in axis_std.values())

    if not baseline_ok:
        axis_z = {}
        drift_level = "insufficient_history"
    else:
        axis_z = {}
        for a, v in axis_scores.items():
            mu = float(axis_mean.get(a, 0.0))
            sd = float(axis_std.get(a, 0.0))
            axis_z[a] = float(_zscore(float(v), mu, sd))

        drift_level = classify_drift(axis_z)

    cat_z = {}
    for cat, v in cat_density.items():
        if cat in cat_mean:
            cat_z[cat] = float(_zscore(float(v), float(cat_mean.get(cat, 0.0)), float(cat_std.get(cat, 0.0))))

    # 4) conversation hooks
    hooks = build_hooks(axis_scores, axis_z, cat_density, cfg)

    # 5) evidence snippets — FULL TEXT ONLY (receipts)
    # axis evidence
    for axis, meta in cfg.axes.items():
        pos_cat = meta.get("positive")
        neg_cat = meta.get("negative")
        axis_val = axis_scores.get(axis, 0.0)

        favored_cat = pos_cat if axis_val >= 0 else neg_cat
        favored_kws = (cfg.categories.get(favored_cat, {}) or {}).get("keywords", []) if favored_cat else []
        snips = find_keyword_snippets(full_text, favored_kws, MAX_EVIDENCE_PER_AXIS)
        write_evidence(conn, video_id, channel_id, axis, favored_cat, snips)

    # top categories evidence
    for cat, _v in _top_n_dict(cat_density, 3):
        kws = (cfg.categories.get(cat, {}) or {}).get("keywords", [])
        snips = find_keyword_snippets(full_text, kws, MAX_EVIDENCE_PER_CATEGORY)
        write_evidence(conn, video_id, channel_id, None, cat, snips)

    # 6) persist results
    top_cats = [k for k, _ in _top_n_dict(cat_density, 5)]

    raw = {
        "video_id": video_id,
        "channel_id": channel_id,
        "title": title,
        "published_at": published_at,
        "word_count": word_count,
        "analysis_source": analysis_source,
        "analysis_word_count": analysis_word_count,
        "category_counts": cat_counts,
        "category_density": cat_density,
        "axis_scores": axis_scores,
        "theological_density": theo_density,
        "scripture_refs": scripture_refs,
        "intent_vectors": intent_vectors,
        "tone_profile": tone_profile,
        "baseline": {
            "window_n": baseline_window,
            "mean": {"axes": axis_mean, "categories": cat_mean},
            "std": {"axes": axis_std, "categories": cat_std},
        },
        "zscores": {"axes": axis_z, "categories": cat_z},
        "drift_level": drift_level,
        "hooks": hooks,
        "signals": signals,
        "version": "brain_v2.4_intent_vectors",
        "analyzed_at": datetime.utcnow().isoformat() + "Z",
    }

    conn.execute("""
    INSERT INTO brain_results (
      video_id,
      theological_density,
      grace_vs_effort,
      hope_vs_fear,
      doctrine_vs_experience,
      scripture_vs_story,
      top_categories,
      raw_scores_json
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(video_id) DO UPDATE SET
      theological_density=excluded.theological_density,
      grace_vs_effort=excluded.grace_vs_effort,
      hope_vs_fear=excluded.hope_vs_fear,
      doctrine_vs_experience=excluded.doctrine_vs_experience,
      scripture_vs_story=excluded.scripture_vs_story,
      top_categories=excluded.top_categories,
      raw_scores_json=excluded.raw_scores_json,
      analyzed_at=CURRENT_TIMESTAMP
    """, (
        video_id,
        float(theo_density),
        float(axis_scores.get("grace_vs_effort", 0.0)),
        float(axis_scores.get("hope_vs_fear", 0.0)),
        float(axis_scores.get("doctrine_vs_experience", 0.0)),
        float(axis_scores.get("scripture_vs_story", 0.0)),
        json.dumps(top_cats),
        json.dumps(raw),
    ))

    # Update baseline cache after insert/update
    axis_mean2, axis_std2, cat_mean2, cat_std2 = compute_channel_baseline(conn, channel_id, baseline_window)
    upsert_baseline(conn, channel_id, baseline_window, axis_mean2, axis_std2, cat_mean2, cat_std2)

    conn.commit()


def run(limit: Optional[int] = None, baseline_window: int = DEFAULT_BASELINE_WINDOW, recompute: bool = False, video_ids: Optional[List[str]] = None) -> None:
    cfg = load_brain_config()
    conn = _connect()
    ensure_tables(conn)

    rows = fetch_unanalyzed_transcripts(conn, limit=limit, recompute=recompute, video_ids=video_ids)
    if not rows:
        print("No unanalyzed transcripts found.")
        return

    processed = 0
    for r in rows:
        analyze_one(
            conn=conn,
            cfg=cfg,
            video_id=r["video_id"],
            channel_id=r["channel_id"],
            title=r["title"] or "",
            published_at=r["published_at"] or "",
            full_text=r["full_text"] or "",
            summary_text=(r["summary_text"] or ""),
            word_count=int(r["word_count"] or 0),
            baseline_window=baseline_window,
        )
        processed += 1

    print(f"Brain v2 complete. Processed {processed} transcripts.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--baseline", type=int, default=DEFAULT_BASELINE_WINDOW)
    parser.add_argument("--recompute", action="store_true")
    parser.add_argument("--video_id", action="append", help="Specific video ID(s) to process (can be used multiple times)")
    args = parser.parse_args()

    run(limit=args.limit, baseline_window=args.baseline, recompute=args.recompute, video_ids=args.video_id)