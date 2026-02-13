import sqlite3
import logging
from typing import Dict, List, Optional, Tuple

from engine.config import DATABASE_PATH

logger = logging.getLogger("digital_pulpit")

# Cache table columns to avoid repeated PRAGMA calls
_TABLE_COL_CACHE: Dict[str, List[str]] = {}


def get_conn():
    conn = sqlite3.connect(DATABASE_PATH)
    return conn


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
      - url OR channel_url
      - resolution_method OR resolved_method OR resolution_source (if present)
    """
    conn = get_conn()
    cols = _table_columns(conn, "channels")

    col_name = _pick_col(cols, ["name", "channel_name"])
    col_url = _pick_col(cols, ["url", "channel_url"])
    col_method = _pick_col(cols, [
        "resolution_method", "resolved_method", "resolution_source", "method"
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
        # Nothing to update besides channel_id; do a simple insert-or-ignore
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

    # Only include columns that exist
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

    # discovered_at / updated_at if present
    if "discovered_at" in cols:
        insert_cols.append("discovered_at")
        # Use SQL function, not a bound param
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
    Schema-adaptive transcript insert. Inserts only columns that exist.
    """
    conn = get_conn()
    cols = _table_columns(conn, "transcripts")

    insert_cols = ["video_id"]
    insert_vals = [video_id]

    mapping = [
        ("transcript_text", transcript_text),
        ("segments_json", segments_json),
        ("language", language),
        ("word_count", word_count),
        ("model", model),
    ]

    for col, val in mapping:
        if col in cols:
            insert_cols.append(col)
            insert_vals.append(val)

    sql = f"INSERT INTO transcripts ({', '.join(insert_cols)}) VALUES ({', '.join(['?']*len(insert_cols))})"
    conn.execute(sql, insert_vals)

    conn.commit()
    conn.close()
