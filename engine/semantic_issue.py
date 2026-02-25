#!/usr/bin/env python3
"""
engine/semantic_issue.py

Bottom-up "Issue" generator with LOGICAL SERMON COLLAPSING.

What it does:
- Reads sermon_analysis (claims + receipts + thesis)
- COLLAPSES multi-part / split broadcasts into one "logical sermon"
  (same channel + normalized base title, within a configurable day gap)
- Clusters key_claims into semantic clusters (embeddings + cosine)
- Builds readable Issue report with representative logical sermons + receipts
- Writes a DOCX using engine.doc_writer.write_doc
- Optionally writes JSON to out/

Run:
  python -m engine.semantic_issue --days 30 --sermon_limit 120 --top 10 --min_size 2 --threshold 0.79 --write_json

Key knobs:
  --collapse_gap_days 10   (default: 10 days between parts in same logical sermon)
  --min_size 2            (cluster min claim count)
  --threshold 0.79        (cosine similarity threshold for greedy clustering)

Notes:
- This clusters CLAIMS (not raw quotes).
- Receipts are pulled from sermon_analysis.receipts_json (already curated by sermon_analyst).
- "Sermon" counts in output now refer to LOGICAL sermons (collapsed), not episodes.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from engine.config import DATABASE_PATH
from engine.doc_writer import write_doc


# ----------------------------
# OpenAI embeddings
# ----------------------------

def _get_openai_client():
    try:
        from openai import OpenAI  # type: ignore
    except Exception as e:
        raise RuntimeError("Missing openai package. Install: pip install openai") from e
    return OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))


def embed_texts(texts: List[str], model: str = "text-embedding-3-small", batch_size: int = 64) -> np.ndarray:
    client = _get_openai_client()
    vecs: List[List[float]] = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        resp = client.embeddings.create(model=model, input=batch)
        for item in resp.data:
            vecs.append(item.embedding)
    return np.array(vecs, dtype=np.float32)


def _cos(a: np.ndarray, b: np.ndarray) -> float:
    denom = (np.linalg.norm(a) * np.linalg.norm(b)) + 1e-8
    return float(np.dot(a, b) / denom)


# ----------------------------
# DB
# ----------------------------

def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


# ----------------------------
# Parsing / normalization
# ----------------------------

def parse_dt(s: str) -> Optional[datetime]:
    """
    Robust-ish datetime parse for common SQLite stored formats:
    - 'YYYY-MM-DD HH:MM:SS'
    - 'YYYY-MM-DDTHH:MM:SS'
    - with or without 'Z'
    """
    if not s:
        return None
    s = s.strip()
    s = s.replace("Z", "")
    # normalize T to space for strptime formats
    s2 = s.replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(s2, fmt)
        except Exception:
            pass
    # last resort: try fromisoformat
    try:
        return datetime.fromisoformat(s2)
    except Exception:
        return None


PART_PATTERNS = [
    # "- Part A", "— Part B", "– Part 2"
    r"\s*[-–—]\s*part\s*([a-z]|\d+)\s*$",
    # "(Part 1)", "[Part 2]"
    r"\s*[\(\[\{]\s*part\s*(\d+|[a-z])\s*[\)\]\}]\s*$",
    # "Part 1" at end
    r"\s*part\s*(\d+|[a-z])\s*$",
    # "Episode 12" at end
    r"\s*episode\s*\d+\s*$",
    # "Week 3" at end
    r"\s*week\s*\d+\s*$",
    # "Session 2" at end
    r"\s*session\s*\d+\s*$",
]


def normalize_title(title: str) -> str:
    """
    Strip common split-broadcast markers so Part A / Part B collapses.
    Also collapses trivial whitespace.
    """
    t = (title or "").strip()
    t = re.sub(r"\s+", " ", t)
    # remove trailing part markers repeatedly (some titles have multiple suffixes)
    changed = True
    while changed:
        changed = False
        for pat in PART_PATTERNS:
            new_t = re.sub(pat, "", t, flags=re.IGNORECASE).strip()
            if new_t != t:
                t = new_t
                changed = True
    return t


# ----------------------------
# Data model
# ----------------------------

@dataclass
class Episode:
    video_id: str
    title: str
    published_at: str


@dataclass
class LogicalSermon:
    logical_id: str
    channel_name: str
    base_title: str
    published_at_min: str
    published_at_max: str
    core_thesis: str
    claims: List[str]
    receipts: List[Dict[str, Any]]
    episodes: List[Episode]


@dataclass
class ClaimItem:
    claim: str
    logical_id: str
    channel_name: str
    base_title: str
    published_at_min: str
    published_at_max: str


@dataclass
class Cluster:
    centroid: np.ndarray
    items: List[ClaimItem]


# ----------------------------
# Fetch analyses
# ----------------------------

def fetch_rows(conn: sqlite3.Connection, days: int, sermon_limit: int) -> List[sqlite3.Row]:
    now = datetime.utcnow()
    start = (now - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")

    q = """
    SELECT
      sa.video_id,
      sa.analysis_json,
      sa.claims_json,
      sa.receipts_json,
      v.title,
      v.published_at,
      c.channel_name
    FROM sermon_analysis sa
    JOIN videos v ON v.video_id = sa.video_id
    LEFT JOIN channels c ON c.channel_id = v.channel_id
    WHERE v.published_at >= ?
    ORDER BY c.channel_name ASC, v.published_at ASC
    LIMIT ?
    """
    return conn.execute(q, (start, sermon_limit)).fetchall()


def _safe_json_load(s: str, default):
    try:
        x = json.loads(s or "")
        return x
    except Exception:
        return default


# ----------------------------
# Logical sermon collapsing
# ----------------------------

def collapse_to_logical_sermons(
    rows: List[sqlite3.Row],
    collapse_gap_days: int = 10,
    max_receipts_per_logical: int = 10
) -> List[LogicalSermon]:
    """
    Collapse episodes into logical sermons by:
      1) channel_name
      2) normalized base_title
      3) split into separate logical sermons if time gap between consecutive episodes > collapse_gap_days
    """
    # group by (channel, base_title) then split by gap
    bucket: Dict[Tuple[str, str], List[sqlite3.Row]] = {}
    for r in rows:
        channel = (r["channel_name"] or "").strip()
        title = r["title"] or ""
        base = normalize_title(title)
        bucket.setdefault((channel, base), []).append(r)

    logical_sermons: List[LogicalSermon] = []

    for (channel, base_title), items in bucket.items():
        # items already sorted by published_at ASC from query, but ensure:
        items = sorted(items, key=lambda x: (x["published_at"] or ""))

        groups: List[List[sqlite3.Row]] = []
        cur: List[sqlite3.Row] = []

        prev_dt: Optional[datetime] = None
        for r in items:
            dt = parse_dt(r["published_at"] or "")
            if not cur:
                cur = [r]
                prev_dt = dt
                continue

            if prev_dt and dt:
                if (dt - prev_dt).days > collapse_gap_days:
                    groups.append(cur)
                    cur = [r]
                else:
                    cur.append(r)
            else:
                # if parsing fails, just keep together
                cur.append(r)

            prev_dt = dt

        if cur:
            groups.append(cur)

        # convert each group to one LogicalSermon
        for gi, grp in enumerate(groups, start=1):
            video_ids = [g["video_id"] for g in grp]
            # logical id stable-ish: channel|base|first_video
            logical_id = f"{channel}::{base_title}::{video_ids[0]}"

            # core thesis: choose the longest non-empty (often best)
            theses: List[str] = []
            all_claims: List[str] = []
            all_receipts: List[Dict[str, Any]] = []
            episodes: List[Episode] = []

            pubs: List[str] = []

            for g in grp:
                pubs.append(g["published_at"] or "")

                episodes.append(Episode(
                    video_id=g["video_id"],
                    title=g["title"] or "",
                    published_at=g["published_at"] or "",
                ))

                analysis = _safe_json_load(g["analysis_json"], {})
                th = (analysis.get("core_thesis") or "").strip()
                if th:
                    theses.append(th)

                claims = _safe_json_load(g["claims_json"], [])
                if isinstance(claims, list):
                    for c in claims:
                        c = (c or "").strip()
                        if len(c) >= 25:
                            all_claims.append(c)

                receipts = _safe_json_load(g["receipts_json"], [])
                if isinstance(receipts, list):
                    for rec in receipts:
                        if isinstance(rec, dict):
                            ex = (rec.get("excerpt") or "").strip()
                            if ex:
                                all_receipts.append(rec)

            # dedupe claims (preserve order)
            seen = set()
            dedup_claims: List[str] = []
            for c in all_claims:
                key = c.lower()
                if key in seen:
                    continue
                seen.add(key)
                dedup_claims.append(c)

            # dedupe receipts by excerpt text
            seen_r = set()
            dedup_receipts: List[Dict[str, Any]] = []
            for rec in all_receipts:
                ex = (rec.get("excerpt") or "").strip()
                k = ex.lower()
                if k in seen_r:
                    continue
                seen_r.add(k)
                dedup_receipts.append(rec)

            dedup_receipts = dedup_receipts[:max_receipts_per_logical]

            # thesis pick
            core_thesis = max(theses, key=len) if theses else ""

            published_at_min = min(pubs) if pubs else ""
            published_at_max = max(pubs) if pubs else ""

            logical_sermons.append(LogicalSermon(
                logical_id=logical_id,
                channel_name=channel,
                base_title=base_title,
                published_at_min=published_at_min,
                published_at_max=published_at_max,
                core_thesis=core_thesis,
                claims=dedup_claims,
                receipts=dedup_receipts,
                episodes=episodes,
            ))

    # sort logical sermons by most recent end date (desc)
    logical_sermons.sort(key=lambda ls: ls.published_at_max, reverse=True)
    return logical_sermons


def logical_sermons_to_claim_items(logicals: List[LogicalSermon]) -> List[ClaimItem]:
    out: List[ClaimItem] = []
    for ls in logicals:
        for c in ls.claims:
            out.append(ClaimItem(
                claim=c,
                logical_id=ls.logical_id,
                channel_name=ls.channel_name,
                base_title=ls.base_title,
                published_at_min=ls.published_at_min,
                published_at_max=ls.published_at_max,
            ))
    return out


# ----------------------------
# Clustering
# ----------------------------

def cluster_greedy(
    items: List[ClaimItem],
    vecs: np.ndarray,
    threshold: float = 0.79,
    min_size: int = 2
) -> Tuple[List[Cluster], Dict[str, Any]]:
    """
    Greedy clustering by centroid similarity.

    Returns clusters plus diagnostics so you never get "blank file surprise".
    """
    clusters: List[Cluster] = []
    for idx, v in enumerate(vecs):
        best_j = -1
        best_sim = -1.0
        for j, c in enumerate(clusters):
            sim = _cos(v, c.centroid)
            if sim > best_sim:
                best_sim = sim
                best_j = j
        if best_j >= 0 and best_sim >= threshold:
            c = clusters[best_j]
            c.items.append(items[idx])
            c.centroid = np.mean(np.vstack([c.centroid, v]), axis=0).astype(np.float32)
        else:
            clusters.append(Cluster(centroid=v.astype(np.float32), items=[items[idx]]))

    # diagnostics before filtering
    sizes = sorted([len(c.items) for c in clusters], reverse=True)
    diag = {
        "raw_clusters": len(clusters),
        "largest_raw_cluster": sizes[0] if sizes else 0,
        "raw_size_counts": {
            str(k): sizes.count(k) for k in sorted(set(sizes))
        }
    }

    clusters = [c for c in clusters if len(c.items) >= min_size]
    clusters.sort(key=lambda c: len(c.items), reverse=True)

    diag["kept_clusters"] = len(clusters)
    diag["min_size"] = min_size
    diag["threshold"] = threshold
    diag["largest_kept_cluster"] = len(clusters[0].items) if clusters else 0

    return clusters, diag


def keyword_label(claims: List[str], k: int = 5) -> str:
    stop = set("""
    the a an and or but if then so to of in for with on at as is are was were be been being
    we you they he she it our your their this that these those not from by into
    must will should can could would
    """.split())
    freq: Dict[str, int] = {}
    for c in claims:
        s = "".join(ch.lower() if ch.isalnum() or ch.isspace() else " " for ch in c)
        for w in s.split():
            if len(w) < 4 or w in stop:
                continue
            freq[w] = freq.get(w, 0) + 1
    top = sorted(freq.items(), key=lambda x: x[1], reverse=True)[:k]
    return ", ".join([w for w, _ in top]) if top else "theme"


# ----------------------------
# Build Issue Report
# ----------------------------

def build_issue_report(
    conn: sqlite3.Connection,
    days: int,
    sermon_limit: int,
    top: int,
    threshold: float,
    min_size: int,
    collapse_gap_days: int,
    reps_per_cluster: int = 2,
    receipts_per_logical: int = 2,
    write_diagnostics: bool = True
) -> Tuple[List[str], List[Dict[str, Any]]]:
    rows = fetch_rows(conn, days=days, sermon_limit=sermon_limit)
    if not rows:
        return (["No sermon analyses found in this window. Run engine.sermon_analyst first."], [])

    logicals = collapse_to_logical_sermons(rows, collapse_gap_days=collapse_gap_days)
    claim_items = logical_sermons_to_claim_items(logicals)

    if not claim_items:
        return (["No claims found. Ensure sermon_analyst produced claims_json."], [])

    vecs = embed_texts([it.claim for it in claim_items], model="text-embedding-3-small")
    clusters, diag = cluster_greedy(claim_items, vecs, threshold=threshold, min_size=min_size)
    clusters = clusters[:top]

    # lookup logical sermon details
    logical_by_id = {ls.logical_id: ls for ls in logicals}

    # Render lines
    stamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    lines: List[str] = []
    lines.append("# Digital Pulpit — Semantic Issue")
    lines.append(f"Generated: {stamp}")
    lines.append(f"Window: last {days} days")
    lines.append(f"Analyses in window: {len(rows)} episodes")
    lines.append(f"Logical sermons (collapsed): {len(logicals)}")
    lines.append(f"Total claims clustered: {len(claim_items)}")
    lines.append(f"Collapse gap: {collapse_gap_days} days")
    lines.append(f"Clustering: threshold {threshold}, min_size {min_size}")
    lines.append("")

    if write_diagnostics:
        lines.append("## Diagnostics")
        lines.append(f"- Raw clusters formed: {diag.get('raw_clusters')}")
        lines.append(f"- Largest raw cluster: {diag.get('largest_raw_cluster')}")
        lines.append(f"- Kept clusters: {diag.get('kept_clusters')}")
        lines.append(f"- Largest kept cluster: {diag.get('largest_kept_cluster')}")
        lines.append("")
        lines.append("---")
        lines.append("")

    lines.append("## Theme Convergence (Bottom-up)")
    lines.append("")

    payload: List[Dict[str, Any]] = []

    for idx, c in enumerate(clusters, start=1):
        claims = [it.claim for it in c.items]
        label = keyword_label(claims[:40])

        logical_ids = sorted({it.logical_id for it in c.items})
        channels = sorted({it.channel_name for it in c.items if it.channel_name})

        # pick representative logical sermons
        reps: List[LogicalSermon] = []
        for lid in logical_ids:
            if lid in logical_by_id:
                reps.append(logical_by_id[lid])
            if len(reps) >= reps_per_cluster:
                break

        lines.append(f"### {idx}. {label}")
        lines.append(f"- Claim occurrences: {len(c.items)}")
        lines.append(f"- Logical sermons: {len(logical_ids)}")
        lines.append(f"- Channels: {len(channels)}")
        lines.append("")

        lines.append("**Cluster signal (representative claims):**")
        for cl in claims[:3]:
            lines.append(f"- {cl}")
        lines.append("")

        lines.append("**Representative logical sermons:**")
        rep_payload: List[Dict[str, Any]] = []
        for ls in reps:
            date_span = ls.published_at_min
            if ls.published_at_max and ls.published_at_max != ls.published_at_min:
                date_span = f"{ls.published_at_min} → {ls.published_at_max}"

            lines.append(f"- {ls.base_title} — {ls.channel_name} ({date_span})")
            if len(ls.episodes) > 1:
                lines.append(f"  Episodes merged: {len(ls.episodes)}")
                for ep in ls.episodes[:4]:
                    lines.append(f"  - {ep.title} ({ep.published_at})")
                if len(ls.episodes) > 4:
                    lines.append(f"  - … +{len(ls.episodes) - 4} more")
            # receipts
            for rec in ls.receipts[:receipts_per_logical]:
                ex = (rec.get("excerpt") or "").strip()
                if ex:
                    lines.append(f"  > {ex}")
            lines.append("")

            rep_payload.append({
                "logical_id": ls.logical_id,
                "base_title": ls.base_title,
                "channel_name": ls.channel_name,
                "published_at_min": ls.published_at_min,
                "published_at_max": ls.published_at_max,
                "episodes": [ep.__dict__ for ep in ls.episodes],
                "core_thesis": ls.core_thesis,
                "sample_receipts": ls.receipts[:receipts_per_logical],
            })

        lines.append("---")
        lines.append("")

        payload.append({
            "cluster_id": idx,
            "label": label,
            "claim_count": len(c.items),
            "logical_sermon_count": len(logical_ids),
            "channel_count": len(channels),
            "representative_claims": claims[:6],
            "representatives": rep_payload,
        })

    return lines, payload


# ----------------------------
# CLI
# ----------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Generate bottom-up Semantic Issue report (with logical sermon collapsing).")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--sermon_limit", type=int, default=120, help="Max episodes scanned (not logical sermons).")
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--threshold", type=float, default=0.79)
    ap.add_argument("--min_size", type=int, default=2)
    ap.add_argument("--collapse_gap_days", type=int, default=10)
    ap.add_argument("--reps_per_cluster", type=int, default=2)
    ap.add_argument("--receipts_per_logical", type=int, default=2)
    ap.add_argument("--write_json", action="store_true", help="Also write structured clusters JSON to out/")
    args = ap.parse_args()

    if not os.environ.get("OPENAI_API_KEY"):
        raise SystemExit("OPENAI_API_KEY is not set in environment (needed for embeddings).")

    conn = connect()
    if not _table_exists(conn, "sermon_analysis"):
        raise SystemExit("sermon_analysis table not found. Run migration + sermon_analyst first.")

    lines, payload = build_issue_report(
        conn=conn,
        days=args.days,
        sermon_limit=args.sermon_limit,
        top=args.top,
        threshold=args.threshold,
        min_size=args.min_size,
        collapse_gap_days=args.collapse_gap_days,
        reps_per_cluster=args.reps_per_cluster,
        receipts_per_logical=args.receipts_per_logical,
    )
    conn.close()

    doc_path = write_doc(lines)
    print(f"\nWord document created: {doc_path}")

    if args.write_json:
        out_dir = Path("out")
        out_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        out_path = out_dir / f"semantic_issue_{stamp}.json"
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"JSON written: {out_path}")


if __name__ == "__main__":
    main()