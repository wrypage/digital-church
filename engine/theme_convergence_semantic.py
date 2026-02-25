#!/usr/bin/env python3
"""
engine/theme_convergence_semantic.py

Clusters bottom-up sermon_analysis key_claims into semantic theme clusters.

Reads:
  sermon_analysis.claims_json
Joins:
  videos, channels (for metadata)

Outputs:
  out/semantic_themes_<timestamp>.json

Run:
  python -m engine.theme_convergence_semantic --days 30 --limit 200 --top 6
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

import numpy as np

from engine.config import DATABASE_PATH


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


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _cos(a: np.ndarray, b: np.ndarray) -> float:
    denom = (np.linalg.norm(a) * np.linalg.norm(b)) + 1e-8
    return float(np.dot(a, b) / denom)


@dataclass
class ClaimItem:
    claim: str
    video_id: str
    title: str
    channel_name: str
    published_at: str


@dataclass
class Cluster:
    centroid: np.ndarray
    items: List[ClaimItem]


def cluster_greedy(items: List[ClaimItem], vecs: np.ndarray, threshold: float = 0.83, min_size: int = 4) -> List[Cluster]:
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

    clusters = [c for c in clusters if len(c.items) >= min_size]
    clusters.sort(key=lambda c: len(c.items), reverse=True)
    return clusters


def _keyword_label(claims: List[str], k: int = 5) -> str:
    stop = set("""
    the a an and or but if then so to of in for with on at as is are was were be been being
    we you they he she it our your their this that these those not from by into
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


def fetch_claims(conn: sqlite3.Connection, days: int, limit: int) -> List[ClaimItem]:
    now = datetime.utcnow()
    start = (now - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")

    q = """
    SELECT
      sa.video_id,
      sa.claims_json,
      v.title,
      v.published_at,
      c.channel_name
    FROM sermon_analysis sa
    JOIN videos v ON v.video_id = sa.video_id
    LEFT JOIN channels c ON c.channel_id = v.channel_id
    WHERE v.published_at >= ?
    ORDER BY v.published_at DESC
    LIMIT ?
    """
    rows = conn.execute(q, (start, limit)).fetchall()

    out: List[ClaimItem] = []
    for r in rows:
        claims = []
        try:
            claims = json.loads(r["claims_json"] or "[]")
        except Exception:
            claims = []
        if not isinstance(claims, list):
            continue
        for cl in claims:
            cl = (cl or "").strip()
            if len(cl) < 20:
                continue
            out.append(ClaimItem(
                claim=cl,
                video_id=r["video_id"],
                title=r["title"] or "",
                channel_name=r["channel_name"] or "",
                published_at=r["published_at"] or "",
            ))
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Cluster sermon_analysis claims into semantic theme clusters.")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--limit", type=int, default=200, help="Max sermons to scan (not claims).")
    ap.add_argument("--top", type=int, default=6, help="Top clusters to output.")
    ap.add_argument("--threshold", type=float, default=0.83)
    ap.add_argument("--min_size", type=int, default=4)
    ap.add_argument("--out_dir", type=str, default="out")
    args = ap.parse_args()

    if not os.environ.get("OPENAI_API_KEY"):
        raise SystemExit("OPENAI_API_KEY is not set in environment.")

    conn = _connect()
    items = fetch_claims(conn, days=args.days, limit=args.limit)
    conn.close()

    if not items:
        print("No claims found. Run sermon_analyst first.")
        return

    texts = [it.claim for it in items]
    vecs = embed_texts(texts, model="text-embedding-3-small")
    clusters = cluster_greedy(items, vecs, threshold=args.threshold, min_size=args.min_size)

    clusters = clusters[:args.top]
    payload: List[Dict[str, Any]] = []
    for i, c in enumerate(clusters, start=1):
        claims = [it.claim for it in c.items]
        label = _keyword_label(claims[:30])
        sermons = sorted({it.video_id for it in c.items})
        channels = sorted({it.channel_name for it in c.items if it.channel_name})

        reps = []
        # representative items: first 6 (you can improve later by centroid distance)
        for it in c.items[:6]:
            reps.append({
                "claim": it.claim,
                "video_id": it.video_id,
                "title": it.title,
                "channel_name": it.channel_name,
                "published_at": it.published_at,
            })

        payload.append({
            "cluster_id": i,
            "label": label,
            "claim_count": len(c.items),
            "sermon_count": len(sermons),
            "channel_count": len(channels),
            "representatives": reps,
        })

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"semantic_themes_{stamp}.json"
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(str(out_path))


if __name__ == "__main__":
    main()