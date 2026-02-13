import sqlite3
import logging
from typing import Dict, List, Optional

from engine.config import DATABASE_PATH

logger = logging.getLogger("digital_pulpit")

# Cache table columns to avoid repeated PRAGMA calls
_TABLE_COL_CACHE: Dict[str, List[str]] = {}


def get_conn():
    """
    Resilient SQLite connection for concurrent-ish workloads (Streamlit + pipeline).
    WAL + busy_timeout reduces 'database is locked' errors significantly.
    """
    conn = sqlite3.connect(DATABASE_PATH, timeout=30)
    # Pragmas (best-effort; some return values vary by SQLite build)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=8000")  # milliseconds
    except Exception:
        pass
    return conn


# Backwards compatibility: older code may call get_connection()
def get_connection():
    return get_conn()


def _table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    if table in _TABLE_COL_CACHE:
        return _TABLE_COL_CACHE[table]
    cols = [
        row[1]
        for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    ]
    _TABLE_COL_CACHE[table] = cols
    return cols


def _pick_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in cols:
            return c
    return None


# ---------------- RUNS ----------------


def create_run(run_type: str) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO runs (run_type, status)
        VALUES (?, 'running')
        """,
        (run_type, ),
    )
    run_id = cur.lastrowid
    conn.commit()
    conn.close()
    return run_id


def finish_run(run_id: int, status: str, videos_processed: int,
               minutes_processed: float, notes: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE runs
        SET
            status = ?,
            finished_at = CURRENT_TIMESTAMP,
            videos_processed = ?,
            minutes_processed = ?,
            notes = ?
        WHERE run_id = ?
        """,
        (status, videos_processed, minutes_processed, notes, run_id),
    )
    conn.commit()
    conn.close()


# ---------------- CHANNELS ----------------


def upsert_channel(channel_id: str, name: str, url: str, method: str):
    """
    Schema-adaptive channel upsert.

    Supports common column variants:
      - name OR channel_name
      - url OR channel_url OR source_url
      - resolution_method OR resolved_via OR resolved_method OR resolution_source (if present)
    """
    conn = get_conn()
    cols = _table_columns(conn, "channels")

    col_name = _pick_col(cols, ["name", "channel_name"])
    col_url = _pick_col(cols, ["url", "channel_url", "source_url"])
    col_method = _pick_col(cols, [
        "resolution_method", "resolved_via", "resolved_method",
        "resolution_source", "method"
    ])

    insert_cols = ["channel_id"]
    insert_vals = [channel_id]
    update_sets = []

    if col_name:
        insert_cols.append(col_name)
        insert_vals.append(name)
        update_sets.append(f"{col_name}=excluded.{col_name}")

    if col_url:
        insert_cols.append(col_url)
        insert_vals.append(url)
        update_sets.append(f"{col_url}=excluded.{col_url}")

    if col_method:
        insert_cols.append(col_method)
        insert_vals.append(method)
        update_sets.append(f"{col_method}=excluded.{col_method}")

    if not update_sets:
        sql = f"INSERT OR IGNORE INTO channels ({', '.join(insert_cols)}) VALUES ({', '.join(['?']*len(insert_cols))})"
        conn.execute(sql, insert_vals)
        conn.commit()
        conn.close()
        return

    sql = f"""
    INSERT INTO channels ({', '.join(insert_cols)})
    VALUES ({', '.join(['?']*len(insert_cols))})
    ON CONFLICT(channel_id) DO UPDATE SET
        {', '.join(update_sets)}
    """
    conn.execute(sql, insert_vals)
    conn.commit()
    conn.close()


# ---------------- VIDEOS ----------------


def upsert_video(video_id,
                 channel_id,
                 title,
                 published_at,
                 duration_seconds,
                 status="discovered"):
    """
    Backwards-compatible helper expected by engine/youtube.py.
    Ensures the videos row exists (dedupe safe).
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO videos
        (video_id, channel_id, title, published_at, duration_seconds, status)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (video_id, channel_id, title, published_at, duration_seconds, status),
    )
    conn.commit()
    conn.close()


def insert_or_ignore_video(
    video_id: str,
    channel_id: str,
    title: str,
    published_at: str,
    duration_seconds: int,
    status: str = "discovered",
    error_message: Optional[str] = None,
):
    """
    Schema-adaptive insert for videos. Inserts only columns that exist.
    Uses INSERT OR IGNORE (dedupe by primary key video_id).
    """
    conn = get_conn()
    cols = _table_columns(conn, "videos")

    insert_cols = ["video_id"]
    insert_vals = [video_id]

    for col, val in [
        ("channel_id", channel_id),
        ("title", title),
        ("published_at", published_at),
        ("duration_seconds", duration_seconds),
        ("status", status),
        ("error_message", error_message),
    ]:
        if col in cols:
            insert_cols.append(col)
            insert_vals.append(val)

    if "discovered_at" in cols:
        insert_cols.append("discovered_at")
        sql_values = ", ".join(["?"] * (len(insert_cols) - 1) +
                               ["CURRENT_TIMESTAMP"])
        sql = f"INSERT OR IGNORE INTO videos ({', '.join(insert_cols)}) VALUES ({sql_values})"
        conn.execute(sql, insert_vals)
    else:
        sql = f"INSERT OR IGNORE INTO videos ({', '.join(insert_cols)}) VALUES ({', '.join(['?']*len(insert_cols))})"
        conn.execute(sql, insert_vals)

    conn.commit()
    conn.close()


def update_video_status(video_id: str, status: str,
                        error_message: Optional[str]):
    """
    Schema-adaptive status update (updates updated_at if present).
    """
    conn = get_conn()
    cols = _table_columns(conn, "videos")

    if "updated_at" in cols:
        conn.execute(
            """
            UPDATE videos
            SET status = ?, error_message = ?, updated_at = CURRENT_TIMESTAMP
            WHERE video_id = ?
            """,
            (status, error_message, video_id),
        )
    else:
        conn.execute(
            """
            UPDATE videos
            SET status = ?, error_message = ?
            WHERE video_id = ?
            """,
            (status, error_message, video_id),
        )

    conn.commit()
    conn.close()


# ---------------- TRANSCRIPTS ----------------


def insert_transcript(
    video_id: str,
    transcript_text: str,
    segments_json: str,
    language: str,
    word_count: int,
    model: str,
):
    """
    UPSERT transcript by video_id to avoid UNIQUE constraint failures.
    Also helps idempotency when rerunning vacuum.
    """
    conn = get_conn()
    cols = _table_columns(conn, "transcripts")

    # Build column/value lists only for existing columns
    data = {"video_id": video_id}

    if "transcript_text" in cols:
        data["transcript_text"] = transcript_text
    if "segments_json" in cols:
        data["segments_json"] = segments_json
    if "language" in cols:
        data["language"] = language
    if "word_count" in cols:
        data["word_count"] = word_count
    if "model" in cols:
        data["model"] = model

    insert_cols = list(data.keys())
    placeholders = ", ".join(["?"] * len(insert_cols))
    insert_vals = [data[c] for c in insert_cols]

    # Upsert: update all fields except video_id
    update_cols = [c for c in insert_cols if c != "video_id"]
    if update_cols:
        update_set = ", ".join([f"{c}=excluded.{c}" for c in update_cols])
        sql = f"""
        INSERT INTO transcripts ({', '.join(insert_cols)})
        VALUES ({placeholders})
        ON CONFLICT(video_id) DO UPDATE SET
            {update_set}
        """
    else:
        # Only video_id exists (unlikely), just ignore duplicates
        sql = f"""
        INSERT OR IGNORE INTO transcripts ({', '.join(insert_cols)})
        VALUES ({placeholders})
        """

    conn.execute(sql, insert_vals)
    conn.commit()
    conn.close()
