import hashlib
import sqlite3
import logging
from typing import Dict, List, Optional

from engine.config import DATABASE_PATH

logger = logging.getLogger("digital_pulpit")

_TABLE_COL_CACHE: Dict[str, List[str]] = {}


def get_conn():
    conn = sqlite3.connect(DATABASE_PATH, timeout=30)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=8000")
    except Exception:
        pass
    return conn


get_connection = get_conn


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


def migrate_channels_table():
    _TABLE_COL_CACHE.pop("channels", None)
    with get_conn() as conn:
        cur = conn.execute("PRAGMA table_info(channels)")
        existing_cols = {row[1]: row for row in cur.fetchall()}
        if not existing_cols:
            logger.info("No channels table found; schema.sql will create it.")
            return

        has_unique_on_name = False
        idx_rows = conn.execute("PRAGMA index_list(channels)").fetchall()
        for idx_row in idx_rows:
            idx_name = idx_row[1]
            is_unique = idx_row[2]
            if is_unique:
                idx_info = conn.execute(f"PRAGMA index_info({idx_name})").fetchall()
                idx_cols = [r[2] for r in idx_info]
                name_col = None
                for candidate in ["channel_name", "name"]:
                    if candidate in idx_cols:
                        name_col = candidate
                        break
                if name_col and len(idx_cols) == 1:
                    has_unique_on_name = True
                    break

        has_unique_on_url = False
        url_col_name = None
        for candidate in ["source_url", "channel_url", "url"]:
            if candidate in existing_cols:
                url_col_name = candidate
                break
        if url_col_name:
            for idx_row in idx_rows:
                idx_name = idx_row[1]
                is_unique = idx_row[2]
                if is_unique:
                    idx_info = conn.execute(f"PRAGMA index_info({idx_name})").fetchall()
                    idx_cols = [r[2] for r in idx_info]
                    if url_col_name in idx_cols and len(idx_cols) == 1:
                        has_unique_on_url = True
                        break

        needs_migration = has_unique_on_name or (url_col_name and not has_unique_on_url)
        if not needs_migration:
            logger.info("channels table constraints are already correct.")
            return

        logger.info("Migrating channels table: removing UNIQUE on name, adding UNIQUE on url...")

        name_col = _pick_col(list(existing_cols.keys()), ["channel_name", "name"])
        url_col = _pick_col(list(existing_cols.keys()), ["source_url", "channel_url", "url"])
        method_col = _pick_col(list(existing_cols.keys()), [
            "resolved_via", "resolution_method", "resolved_method", "resolution_source", "method"
        ])

        col_defs = ["channel_id TEXT PRIMARY KEY"]
        copy_cols = ["channel_id"]
        if name_col:
            col_defs.append(f"{name_col} TEXT")
            copy_cols.append(name_col)
        if url_col:
            col_defs.append(f"{url_col} TEXT UNIQUE")
            copy_cols.append(url_col)
        if method_col:
            col_defs.append(f"{method_col} TEXT")
            copy_cols.append(method_col)
        if "added_at" in existing_cols:
            col_defs.append("added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            copy_cols.append("added_at")
        if "active" in existing_cols:
            col_defs.append("active INTEGER DEFAULT 1")
            copy_cols.append("active")

        cols_csv = ", ".join(copy_cols)
        col_defs_csv = ", ".join(col_defs)

        conn.execute("PRAGMA foreign_keys=OFF")
        conn.execute("BEGIN")
        try:
            conn.execute(f"CREATE TABLE channels_new ({col_defs_csv})")
            conn.execute(f"INSERT OR IGNORE INTO channels_new ({cols_csv}) SELECT {cols_csv} FROM channels")
            conn.execute("DROP TABLE channels")
            conn.execute("ALTER TABLE channels_new RENAME TO channels")
            conn.execute("COMMIT")
            logger.info("channels table migration complete.")
        except Exception:
            conn.execute("ROLLBACK")
            logger.exception("channels migration failed — rolled back.")
            raise
        finally:
            conn.execute("PRAGMA foreign_keys=ON")


# ---------------- RUNS ----------------


def create_run(run_type: str) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO runs (run_type, status) VALUES (?, 'running')",
            (run_type,),
        )
        return cur.lastrowid


def finish_run(run_id: int, status: str, videos_processed: int,
               minutes_processed: float, notes: str):
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE runs
            SET status = ?, finished_at = CURRENT_TIMESTAMP,
                videos_processed = ?, minutes_processed = ?, notes = ?
            WHERE run_id = ?
            """,
            (status, videos_processed, minutes_processed, notes, run_id),
        )


# ---------------- CHANNELS ----------------


def upsert_channel(channel_id: str, name: str, url: str, method: str):
    with get_conn() as conn:
        cols = _table_columns(conn, "channels")

        col_name = _pick_col(cols, ["name", "channel_name"])
        col_url = _pick_col(cols, ["url", "channel_url", "source_url"])
        col_method = _pick_col(cols, [
            "resolution_method", "resolved_via", "resolved_method",
            "resolution_source", "method"
        ])

        has_channel_id = bool(channel_id and str(channel_id).strip())

        if not has_channel_id:
            if url and str(url).strip():
                channel_id = "URL_" + hashlib.sha256(url.strip().encode()).hexdigest()[:16]
                has_channel_id = True
                logger.info("Generated surrogate channel_id %s for url %s", channel_id, url)
            else:
                logger.warning("No channel_id and no url — cannot upsert channel %s", name)
                return

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

        if update_sets:
            sql = f"""
            INSERT INTO channels ({', '.join(insert_cols)})
            VALUES ({', '.join(['?']*len(insert_cols))})
            ON CONFLICT(channel_id) DO UPDATE SET
                {', '.join(update_sets)}
            """
        else:
            sql = f"INSERT OR IGNORE INTO channels ({', '.join(insert_cols)}) VALUES ({', '.join(['?']*len(insert_cols))})"
        conn.execute(sql, insert_vals)


# ---------------- VIDEOS ----------------


def upsert_video(video_id, channel_id, title, published_at,
                 duration_seconds, status="discovered"):
    with get_conn() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO videos
            (video_id, channel_id, title, published_at, duration_seconds, status)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (video_id, channel_id, title, published_at, duration_seconds, status),
        )


def insert_or_ignore_video(
    video_id: str,
    channel_id: str,
    title: str,
    published_at: str,
    duration_seconds: int,
    status: str = "discovered",
    error_message: Optional[str] = None,
):
    with get_conn() as conn:
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


def update_video_status(video_id: str, status: str,
                        error_message: Optional[str]):
    with get_conn() as conn:
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


# ---------------- TRANSCRIPTS ----------------


def insert_transcript(
    video_id: str,
    transcript_text: str,
    segments_json: str,
    language: str,
    word_count: int,
    model: str,
    provider: str = "openai_api",
):
    with get_conn() as conn:
        cols = _table_columns(conn, "transcripts")

        data = {"video_id": video_id}

        if "transcript_text" in cols:
            data["transcript_text"] = transcript_text
        if "full_text" in cols:
            data["full_text"] = transcript_text
        if "segments_json" in cols:
            data["segments_json"] = segments_json
        if "language" in cols:
            data["language"] = language
        if "word_count" in cols:
            data["word_count"] = word_count
        if "model" in cols:
            data["model"] = model
        if "transcript_model" in cols:
            data["transcript_model"] = model
        if "transcript_provider" in cols:
            data["transcript_provider"] = provider

        insert_cols = list(data.keys())
        placeholders = ", ".join(["?"] * len(insert_cols))
        insert_vals = [data[c] for c in insert_cols]

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
            sql = f"""
            INSERT OR IGNORE INTO transcripts ({', '.join(insert_cols)})
            VALUES ({placeholders})
            """

        conn.execute(sql, insert_vals)
