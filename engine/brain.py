# engine/brain.py
import argparse
import json
import math
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from .config import DATABASE_PATH, load_theology_config

# -----------------------------
# Settings
# -----------------------------
DEFAULT_BASELINE_WINDOW = 4
MAX_EVIDENCE_PER_AXIS = 4
MAX_EVIDENCE_PER_CATEGORY = 3
SNIPPET_WORD_WINDOW = 28  # words around keyword match
MIN_WORD_COUNT = 100      # also enforced by config if present


# -----------------------------
# Utilities
# -----------------------------
def _connect(db_path: str = DATABASE_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
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
# Scripture extraction (basic, fast)
# -----------------------------
# This is intentionally simple; it catches common patterns like:
# "Romans 8", "Romans 8:28", "1 Corinthians 13", "1 Cor 13:4-7", etc.
BOOK_PATTERNS = [
    # Pentateuch / OT (basic)
    ("genesis", [r"genesis", r"\bgen\b"]),
    ("exodus", [r"exodus", r"\bexo\b", r"\bex\b"]),
    ("deuteronomy", [r"deuteronomy", r"\bdeut\b", r"\bdeu\b"]),
    ("psalms", [r"psalms", r"\bpsalm\b", r"\bps\b"]),
    ("proverbs", [r"proverbs", r"\bprov\b", r"\bpr\b"]),
    ("isaiah", [r"isaiah", r"\bisa\b"]),
    ("jeremiah", [r"jeremiah", r"\bjer\b"]),
    # NT (common)
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

# chapter / verse: optional, because sometimes they only say "Romans 8"
CHV_RE = r"(?:\s+(\d{1,3})(?::(\d{1,3})(?:\s*[-–]\s*(\d{1,3}))?)?)?"

def extract_scripture_refs(text: str) -> Dict[str, int]:
    """
    Returns counts by canonical book name. Basic heuristic.
    """
    if not text:
        return {}
    t = text.lower()
    counts: Dict[str, int] = {}
    for book, pats in BOOK_PATTERNS:
        # build a combined pattern
        combined = "(" + "|".join(pats) + ")" + CHV_RE
        matches = re.findall(combined, t, flags=re.IGNORECASE)
        if matches:
            counts[book] = len(matches)
    return counts


# -----------------------------
# Snippet extraction (evidence receipts)
# -----------------------------
def _word_window_excerpt(original_text: str, start_char: int, window_words: int) -> str:
    """
    Extract ~window_words before/after the match by word count (approx).
    """
    if not original_text:
        return ""
    # Create a mapping from char index to word index is expensive.
    # We'll do a simple approach: take a char window, then trim to words.
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
    """
    Returns list of (keyword, excerpt, start_char).
    Finds the first occurrences across keywords, preferring earlier matches.
    """
    if not full_text or not keywords or max_snips <= 0:
        return []

    text = full_text
    lower = text.lower()

    hits: List[Tuple[int, str]] = []
    for kw in keywords:
        kw_l = kw.lower().strip()
        if not kw_l:
            continue
        # word boundary for alphanumerics; allow phrases
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
        # avoid near-duplicate positions
        bucket = pos // 50
        if bucket in seen_positions:
            continue
        seen_positions.add(bucket)
        excerpt = _word_window_excerpt(text, pos, SNIPPET_WORD_WINDOW)
        out.append((kw, excerpt, pos))

    return out


# -----------------------------
# Brain analysis core
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


def score_categories(full_text: str, cfg: BrainConfig) -> Tuple[Dict[str, int], Dict[str, float]]:
    """
    Returns (category_counts, category_density_per_1000_words).
    Counts are keyword hit counts (simple).
    """
    normalized = _normalize_text(full_text)
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
                # phrase count
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
    Axis score in [-1, 1] using normalized difference:
      (pos - neg) / (pos + neg + epsilon)
    """
    out: Dict[str, float] = {}
    eps = 1e-9
    for axis, meta in cfg.axes.items():
        pos = meta.get("positive")
        neg = meta.get("negative")
        pos_v = float(category_density.get(pos, 0.0))
        neg_v = float(category_density.get(neg, 0.0))
        out[axis] = (pos_v - neg_v) / (pos_v + neg_v + eps)
    return out


def theological_density(category_density: Dict[str, float]) -> float:
    # simple: total density across all categories
    return float(sum(category_density.values()))


def compute_signals(category_density: Dict[str, float]) -> Dict:
    """
    Compute Brain v2.1 signals from category densities.
    Returns dict with:
    - immanence_vs_transcendence: axis score [-1, 1]
    - mode_distribution: {prophetic, pastoral, didactic} proportions
    - trinity_distribution: {christ, father, spirit} proportions
    """
    eps = 1e-9

    # 1) immanence_vs_transcendence axis (same formula as other axes)
    imm = float(category_density.get("immanence", 0.0))
    trans = float(category_density.get("transcendence", 0.0))
    imm_vs_trans = (imm - trans) / (imm + trans + eps)

    # 2) mode_distribution (prophetic/pastoral/didactic)
    prophetic = float(category_density.get("prophetic", 0.0))
    pastoral = float(category_density.get("pastoral", 0.0))
    didactic = float(category_density.get("didactic", 0.0))
    mode_total = prophetic + pastoral + didactic + eps
    mode_dist = {
        "prophetic": prophetic / mode_total,
        "pastoral": pastoral / mode_total,
        "didactic": didactic / mode_total,
    }

    # 3) trinity_distribution (christ/father/spirit)
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
# Baseline + drift
# -----------------------------
def compute_channel_baseline(
    conn: sqlite3.Connection,
    channel_id: str,
    window_n: int,
) -> Tuple[Dict[str, float], Dict[str, float], Dict[str, float], Dict[str, float]]:
    """
    Returns (axis_mean, axis_std, cat_mean, cat_std) computed from last N analyzed sermons.
    Axis values come from brain_results columns; category densities from raw_scores_json.
    """
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
    """
    Simple drift classification by max absolute z across axes.
    """
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


def build_hooks(
    axis_scores: Dict[str, float],
    axis_z: Dict[str, float],
    cat_density: Dict[str, float],
    cfg: BrainConfig
) -> Dict:
    """
    Build structured talking points (not opinions).
    """
    hooks: Dict[str, List] = {"tensions": [], "questions": []}

    # Dominant axis by absolute score
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

    # Imbalances: extreme axis score
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

    # Drift flags: axis zscore high
    for a, z in axis_z.items():
        if abs(z) >= 2.0:
            hooks["tensions"].append({
                "type": "drift",
                "axis": a,
                "z": float(z),
                "note": "Axis deviated strongly from channel baseline",
            })

    # Starter questions (generic, grounded)
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
def fetch_unanalyzed_transcripts(conn: sqlite3.Connection, limit: Optional[int] = None, recompute: bool = False):
    q = """
    SELECT
        t.video_id,
        t.full_text,
        t.word_count,
        t.segments_json,
        v.channel_id,
        v.title,
        v.published_at
    FROM transcripts t
    JOIN videos v ON t.video_id = v.video_id
    """

    if not recompute:
        q += """
    LEFT JOIN brain_results br ON t.video_id = br.video_id
    WHERE br.result_id IS NULL
      AND t.full_text IS NOT NULL
      AND t.word_count IS NOT NULL
    """
    else:
        q += """
    WHERE t.full_text IS NOT NULL
      AND t.word_count IS NOT NULL
    """

    q += "ORDER BY v.published_at DESC"

    if limit:
        q += " LIMIT ?"
        return conn.execute(q, (limit,)).fetchall()
    return conn.execute(q).fetchall()


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
    """
    snippets: list of (keyword, excerpt, start_char)
    """
    if not snippets:
        return
    for kw, excerpt, start_char in snippets:
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
# Main analysis pipeline
# -----------------------------
def analyze_one(
    conn: sqlite3.Connection,
    cfg: BrainConfig,
    video_id: str,
    channel_id: str,
    title: str,
    published_at: str,
    full_text: str,
    word_count: int,
    baseline_window: int = DEFAULT_BASELINE_WINDOW,
) -> None:
    if not full_text or (word_count or 0) < cfg.min_word_count:
        return

    # A) Prevent duplicate evidence on reruns
    conn.execute("DELETE FROM brain_evidence WHERE video_id = ?", (video_id,))

    # 1) category + axis scoring
    cat_counts, cat_density = score_categories(full_text, cfg)
    axis_scores = score_axes(cat_density, cfg)
    theo_density = theological_density(cat_density)

    # 1b) Brain v2.1 signals
    signals = compute_signals(cat_density)

    # 2) scripture refs (basic)
    scripture_refs = extract_scripture_refs(full_text)

    # 3) baseline + zscores vs channel history
    axis_mean, axis_std, cat_mean, cat_std = compute_channel_baseline(conn, channel_id, baseline_window)

    # C) Baseline correctness: check if we have valid baseline data
    baseline_ok = bool(axis_mean) and any((float(s) > 1e-9) for s in axis_std.values())

    if not baseline_ok:
        # Insufficient history: set empty zscores and special drift level
        axis_z = {}
        drift_level = "insufficient_history"
    else:
        # Compute zscores normally
        axis_z = {}
        for a, v in axis_scores.items():
            mu = float(axis_mean.get(a, 0.0))
            sd = float(axis_std.get(a, 0.0))
            axis_z[a] = float(_zscore(float(v), mu, sd))

        drift_level = classify_drift(axis_z)

    # category zscores for top cats only (keep it light)
    cat_z = {}
    for cat, v in cat_density.items():
        if cat in cat_mean:
            cat_z[cat] = float(_zscore(float(v), float(cat_mean.get(cat, 0.0)), float(cat_std.get(cat, 0.0))))

    # 4) conversation hooks
    hooks = build_hooks(axis_scores, axis_z, cat_density, cfg)

    # 5) evidence snippets
    # For each axis, collect snippets from favored side keywords to support the direction.
    for axis, meta in cfg.axes.items():
        pos_cat = meta.get("positive")
        neg_cat = meta.get("negative")
        axis_val = axis_scores.get(axis, 0.0)

        favored_cat = pos_cat if axis_val >= 0 else neg_cat
        favored_kws = (cfg.categories.get(favored_cat, {}) or {}).get("keywords", []) if favored_cat else []
        snips = find_keyword_snippets(full_text, favored_kws, MAX_EVIDENCE_PER_AXIS)
        write_evidence(conn, video_id, channel_id, axis, favored_cat, snips)

    # Also store a few category-level snippets for top categories by density
    for cat, _v in _top_n_dict(cat_density, 3):
        kws = (cfg.categories.get(cat, {}) or {}).get("keywords", [])
        snips = find_keyword_snippets(full_text, kws, MAX_EVIDENCE_PER_CATEGORY)
        write_evidence(conn, video_id, channel_id, None, cat, snips)

    # 5b) evidence snippets for signal categories
    # immanence_vs_transcendence axis
    imm_vs_trans_val = signals.get("immanence_vs_transcendence", 0.0)
    imm_favored = "immanence" if imm_vs_trans_val >= 0 else "transcendence"
    imm_kws = (cfg.categories.get(imm_favored, {}) or {}).get("keywords", [])
    if imm_kws:
        snips = find_keyword_snippets(full_text, imm_kws, MAX_EVIDENCE_PER_AXIS)
        write_evidence(conn, video_id, channel_id, "immanence_vs_transcendence", imm_favored, snips)

    # mode_distribution categories (prophetic, pastoral, didactic)
    for mode_cat in ["prophetic", "pastoral", "didactic"]:
        kws = (cfg.categories.get(mode_cat, {}) or {}).get("keywords", [])
        if kws:
            snips = find_keyword_snippets(full_text, kws, MAX_EVIDENCE_PER_CATEGORY)
            write_evidence(conn, video_id, channel_id, None, mode_cat, snips)

    # trinity_distribution categories (christ, father, spirit)
    for trinity_cat in ["christ", "father", "spirit"]:
        kws = (cfg.categories.get(trinity_cat, {}) or {}).get("keywords", [])
        if kws:
            snips = find_keyword_snippets(full_text, kws, MAX_EVIDENCE_PER_CATEGORY)
            write_evidence(conn, video_id, channel_id, None, trinity_cat, snips)

    # 6) persist results
    top_cats = [k for k, _ in _top_n_dict(cat_density, 5)]

    raw = {
        "video_id": video_id,
        "channel_id": channel_id,
        "title": title,
        "published_at": published_at,
        "word_count": word_count,
        "category_counts": cat_counts,
        "category_density": cat_density,
        "axis_scores": axis_scores,
        "theological_density": theo_density,
        "scripture_refs": scripture_refs,
        "baseline": {
            "window_n": baseline_window,
            "mean": {"axes": axis_mean, "categories": cat_mean},
            "std": {"axes": axis_std, "categories": cat_std},
        },
        "zscores": {"axes": axis_z, "categories": cat_z},
        "drift_level": drift_level,
        "hooks": hooks,
        "signals": signals,
        "version": "brain_v2.1",
        "analyzed_at": datetime.utcnow().isoformat() + "Z",
    }

    # B) Make brain_results insert idempotent (UPSERT)
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

    # 7) update baseline cache for this channel (optional but useful)
    # recompute after inserting this sermon so next one has fresher baseline
    axis_mean2, axis_std2, cat_mean2, cat_std2 = compute_channel_baseline(conn, channel_id, baseline_window)
    upsert_baseline(conn, channel_id, baseline_window, axis_mean2, axis_std2, cat_mean2, cat_std2)

    conn.commit()


def run(limit: Optional[int] = None, baseline_window: int = DEFAULT_BASELINE_WINDOW, recompute: bool = False) -> None:
    cfg = load_brain_config()
    conn = _connect()
    ensure_tables(conn)

    rows = fetch_unanalyzed_transcripts(conn, limit=limit, recompute=recompute)
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
    args = parser.parse_args()
    run(limit=args.limit, baseline_window=args.baseline, recompute=args.recompute)
