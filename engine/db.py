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

        conn.execute("ALTER TABLE channels RENAME TO channels_old")
        conn.execute(f"CREATE TABLE channels ({', '.join(col_defs)})")
        conn.execute(
            f"INSERT INTO channels ({', '.join(copy_cols)}) "
            f"SELECT {', '.join(copy_cols)} FROM channels_old"
        )
        conn.execute("DROP TABLE channels_old")
        conn.commit()
        _TABLE_COL_CACHE.pop("channels", None)


def create_run(run_type: str) -> int:
    with get_conn() as conn:
        cur = conn.execute(
            "INSERT INTO runs (run_type, status) VALUES (?, 'running')",
            (run_type,),
        )
        conn.commit()
        return int(cur.lastrowid)


def finish_run(run_id: int, status: str, videos_processed: int, minutes_processed: float, notes: str = "") -> None:
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE runs
               SET finished_at = CURRENT_TIMESTAMP,
                   status = ?,
                   videos_processed = ?,
                   minutes_processed = ?,
                   notes = ?
             WHERE run_id = ?
            """,
            (status, videos_processed, minutes_processed, notes, run_id),
        )
        conn.commit()


def upsert_channel(channel_id: str, channel_name: str, source_url: str, resolved_via: str = "") -> None:
    """
    Upsert a channel with real-world merge behavior.

    Constraints in your schema:
      - channels.channel_id is PRIMARY KEY (UNIQUE)
      - channels.source_url is UNIQUE (seeded from channels.csv)

    Reality:
      - Multiple URLs/handles can resolve to the same channel_id (redirects, rebrands, aliases)

    Rules implemented:
      A) If channel_id already exists -> treat as canonical row; update metadata only (do NOT change PK)
      B) Else if source_url exists -> update that row to the new channel_id (+ metadata)
      C) Else -> insert new row
    """
    with get_conn() as conn:
        cols = _table_columns(conn, "channels")

        name_col = _pick_col(cols, ["channel_name", "name"])
        url_col = _pick_col(cols, ["source_url", "channel_url", "url"])
        via_col = _pick_col(cols, ["resolved_via", "resolution_method", "resolved_method", "method"])

        if not url_col:
            raise ValueError("channels table missing URL column (source_url/channel_url/url)")

        # 1) Does this channel_id already exist? (canonical row)
        existing_by_id = conn.execute(
            "SELECT channel_id FROM channels WHERE channel_id = ?",
            (channel_id,),
        ).fetchone()

        if existing_by_id:
            # Merge metadata into the canonical row, but do NOT touch the primary key.
            set_clauses = []
            params = []

            if name_col:
                set_clauses.append(
                    f"{name_col} = CASE WHEN ? IS NOT NULL AND TRIM(?) <> '' THEN ? ELSE {name_col} END"
                )
                params.extend([channel_name, channel_name, channel_name])

            if via_col:
                set_clauses.append(
                    f"{via_col} = CASE WHEN ? IS NOT NULL AND TRIM(?) <> '' THEN ? ELSE {via_col} END"
                )
                params.extend([resolved_via, resolved_via, resolved_via])

            if set_clauses:
                params.append(channel_id)
                conn.execute(
                    f"UPDATE channels SET {', '.join(set_clauses)} WHERE channel_id = ?",
                    tuple(params),
                )

            conn.commit()
            return

        # 2) Does this URL already exist? (seed row from CSV)
        existing_by_url = conn.execute(
            f"SELECT channel_id FROM channels WHERE {url_col} = ?",
            (source_url,),
        ).fetchone()

        if existing_by_url:
            updates = ["channel_id = ?"]
            params = [channel_id]

            if name_col:
                updates.append(f"{name_col} = ?")
                params.append(channel_name)

            if via_col:
                updates.append(f"{via_col} = ?")
                params.append(resolved_via)

            params.append(source_url)

            conn.execute(
                f"UPDATE channels SET {', '.join(updates)} WHERE {url_col} = ?",
                tuple(params),
            )
            conn.commit()
            return

        # 3) Brand new row -> INSERT
        fields = ["channel_id", url_col]
        values = [channel_id, source_url]

        if name_col:
            fields.append(name_col)
            values.append(channel_name)

        if via_col:
            fields.append(via_col)
            values.append(resolved_via)

        placeholders = ", ".join(["?"] * len(values))

        conn.execute(
            f"INSERT INTO channels ({', '.join(fields)}) VALUES ({placeholders})",
            tuple(values),
        )
        conn.commit()


def upsert_video(
    video_id: str,
    channel_id: str,
    title: str,
    published_at,
    duration_seconds: int,
    status: str = "discovered",
    error_message: Optional[str] = None,
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO videos (video_id, channel_id, title, published_at, duration_seconds, status, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(video_id) DO UPDATE SET
                channel_id=excluded.channel_id,
                title=COALESCE(excluded.title, videos.title),
                published_at=COALESCE(excluded.published_at, videos.published_at),
                duration_seconds=COALESCE(excluded.duration_seconds, videos.duration_seconds),
                status=excluded.status,
                error_message=excluded.error_message,
                updated_at=CURRENT_TIMESTAMP
            """,
            (video_id, channel_id, title, published_at, duration_seconds, status, error_message),
        )
        conn.commit()


def insert_or_ignore_video(
    video_id: str,
    channel_id: str,
    title: str,
    published_at,
    duration_seconds: int,
    status: str = "discovered",
    error_message: Optional[str] = None,
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO videos
              (video_id, channel_id, title, published_at, duration_seconds, status, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (video_id, channel_id, title, published_at, duration_seconds, status, error_message),
        )
        conn.commit()


def update_video_status(video_id: str, status: str, error_message: Optional[str] = None) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE videos
               SET status = ?,
                   error_message = ?,
                   updated_at = CURRENT_TIMESTAMP
             WHERE video_id = ?
            """,
            (status, error_message, video_id),
        )
        conn.commit()


def insert_transcript(
    video_id: str,
    full_text: str,
    segments_json: str,
    language: str,
    word_count: int,
    transcript_provider: str = "openai_api",
    transcript_model: str = "",
    transcript_version: str = "v5.2",
) -> tuple[bool, Optional[str]]:
    if not full_text or not str(full_text).strip():
        return False, "Refusing to insert empty transcript"

    try:
        with get_conn() as conn:
            conn.execute(
                """
                INSERT INTO transcripts
                    (video_id, full_text, segments_json, language, word_count, transcript_provider, transcript_model, transcript_version)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(video_id) DO UPDATE SET
                    full_text=excluded.full_text,
                    segments_json=excluded.segments_json,
                    language=excluded.language,
                    word_count=excluded.word_count,
                    transcript_provider=excluded.transcript_provider,
                    transcript_model=excluded.transcript_model,
                    transcript_version=excluded.transcript_version,
                    transcribed_at=CURRENT_TIMESTAMP
                """,
                (video_id, full_text, segments_json, language, word_count, transcript_provider, transcript_model, transcript_version),
            )
            conn.commit()
        return True, None
    except Exception as e:
        return False, f"Database error: {type(e).__name__}: {str(e)}"


def hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def init_db():
    """Initialize database by running schema.sql"""
    import os
    schema_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "schema.sql")

    if not os.path.exists(schema_path):
        logger.warning(f"schema.sql not found at {schema_path}")
        return

    with open(schema_path, "r") as f:
        schema_sql = f.read()

    with get_conn() as conn:
        conn.executescript(schema_sql)
        conn.commit()

    logger.info("Database initialized from schema.sql")


def get_db_stats() -> dict:
    """Get database statistics"""
    with get_conn() as conn:
        stats = {}

        # Count channels
        stats["channels"] = conn.execute("SELECT COUNT(*) FROM channels").fetchone()[0]

        # Count videos
        stats["videos"] = conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0]

        # Count transcripts
        stats["transcripts"] = conn.execute("SELECT COUNT(*) FROM transcripts").fetchone()[0]

        # Count brain results
        stats["brain_results"] = conn.execute("SELECT COUNT(*) FROM brain_results").fetchone()[0]

        # Video status breakdown
        status_rows = conn.execute(
            "SELECT status, COUNT(*) as count FROM videos GROUP BY status"
        ).fetchall()
        stats["video_statuses"] = {row[0]: row[1] for row in status_rows}

        return stats


def get_recent_runs(limit: int = 10) -> List[dict]:
    """Get recent runs"""
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT run_id, run_type, started_at, finished_at, status,
                   videos_processed, minutes_processed, notes
            FROM runs
            ORDER BY run_id DESC
            LIMIT ?
            """,
            (limit,)
        ).fetchall()

        return [
            {
                "run_id": r[0],
                "run_type": r[1],
                "started_at": r[2],
                "finished_at": r[3],
                "status": r[4],
                "videos_processed": r[5],
                "minutes_processed": r[6],
                "notes": r[7],
            }
            for r in rows
        ]


def get_all_brain_results() -> List[dict]:
    """Get all brain analysis results"""
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT br.video_id, v.title, c.channel_name, v.channel_id,
                   br.theological_density, br.grace_vs_effort, br.hope_vs_fear,
                   br.doctrine_vs_experience, br.scripture_vs_story,
                   br.top_categories, br.analyzed_at
            FROM brain_results br
            LEFT JOIN videos v ON br.video_id = v.video_id
            LEFT JOIN channels c ON v.channel_id = c.channel_id
            ORDER BY br.analyzed_at DESC
            """
        ).fetchall()

        return [
            {
                "video_id": r[0],
                "title": r[1],
                "channel_name": r[2],
                "channel_id": r[3],
                "theological_density": r[4],
                "grace_vs_effort": r[5],
                "hope_vs_fear": r[6],
                "doctrine_vs_experience": r[7],
                "scripture_vs_story": r[8],
                "top_categories": r[9],
                "analyzed_at": r[10],
            }
            for r in rows
        ]


def get_brain_results_for_week(week_start: str, week_end: str) -> List[dict]:
    """
    Get brain analysis results for a specific week.
    Filters by analyzed_at date between week_start and week_end.
    """
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT br.video_id, v.title, c.channel_name, v.channel_id,
                   br.theological_density, br.grace_vs_effort, br.hope_vs_fear,
                   br.doctrine_vs_experience, br.scripture_vs_story,
                   br.top_categories, br.analyzed_at
            FROM brain_results br
            LEFT JOIN videos v ON br.video_id = v.video_id
            LEFT JOIN channels c ON v.channel_id = c.channel_id
            WHERE DATE(br.analyzed_at) BETWEEN ? AND ?
            ORDER BY br.analyzed_at DESC
            """,
            (str(week_start), str(week_end))
        ).fetchall()

        return [
            {
                "video_id": r[0],
                "title": r[1],
                "channel_name": r[2],
                "channel_id": r[3],
                "theological_density": r[4],
                "grace_vs_effort": r[5],
                "hope_vs_fear": r[6],
                "doctrine_vs_experience": r[7],
                "scripture_vs_story": r[8],
                "top_categories": r[9],
                "analyzed_at": r[10],
            }
            for r in rows
        ]


def get_weekly_drift_reports(limit: int = 10) -> List[dict]:
    """Get weekly drift reports"""
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT report_id, week_start, week_end, channel_id,
                   avg_theological_density, grace_vs_effort_zscore,
                   hope_vs_fear_zscore, doctrine_vs_experience_zscore,
                   scripture_vs_story_zscore, sample_size, created_at
            FROM weekly_drift_reports
            ORDER BY week_start DESC
            LIMIT ?
            """,
            (limit,)
        ).fetchall()

        return [
            {
                "report_id": r[0],
                "week_start": r[1],
                "week_end": r[2],
                "channel_id": r[3],
                "avg_theological_density": r[4],
                "grace_vs_effort_zscore": r[5],
                "hope_vs_fear_zscore": r[6],
                "doctrine_vs_experience_zscore": r[7],
                "scripture_vs_story_zscore": r[8],
                "sample_size": r[9],
                "created_at": r[10],
            }
            for r in rows
        ]


def get_assembly_scripts(limit: int = 10) -> List[dict]:
    """Get assembly scripts"""
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT script_id, week_start, week_end, script_text,
                   avatar_assignments_json, source_video_ids, created_at
            FROM assembly_scripts
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,)
        ).fetchall()

        return [
            {
                "script_id": r[0],
                "week_start": r[1],
                "week_end": r[2],
                "script_text": r[3],
                "avatar_assignments_json": r[4],
                "source_video_ids": r[5],
                "created_at": r[6],
            }
            for r in rows
        ]


def get_transcript(video_id: str) -> Optional[dict]:
    """Get transcript for a video"""
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT video_id, full_text, segments_json, language, word_count,
                   transcript_provider, transcript_model, transcript_version, transcribed_at
            FROM transcripts
            WHERE video_id = ?
            """,
            (video_id,)
        ).fetchone()

        if not row:
            return None

        return {
            "video_id": row[0],
            "full_text": row[1],
            "segments_json": row[2],
            "language": row[3],
            "word_count": row[4],
            "transcript_provider": row[5],
            "transcript_model": row[6],
            "transcript_version": row[7],
            "transcribed_at": row[8],
        }


def insert_brain_result(
    video_id: str,
    theological_density: float,
    grace_vs_effort: float,
    hope_vs_fear: float,
    doctrine_vs_experience: float,
    scripture_vs_story: float,
    top_categories: str,
    raw_scores_json: str,
) -> None:
    """Insert brain analysis result"""
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO brain_results
                (video_id, theological_density, grace_vs_effort, hope_vs_fear,
                 doctrine_vs_experience, scripture_vs_story, top_categories, raw_scores_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (video_id, theological_density, grace_vs_effort, hope_vs_fear,
             doctrine_vs_experience, scripture_vs_story, top_categories, raw_scores_json),
        )
        conn.commit()


def get_transcribed_videos_without_analysis() -> List[dict]:
    """Get videos that have transcripts but no brain analysis"""
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT t.video_id, v.title, v.channel_id
            FROM transcripts t
            LEFT JOIN videos v ON t.video_id = v.video_id
            LEFT JOIN brain_results br ON t.video_id = br.video_id
            WHERE br.video_id IS NULL
            ORDER BY t.transcribed_at DESC
            """
        ).fetchall()

        return [
            {
                "video_id": r[0],
                "title": r[1],
                "channel_id": r[2],
            }
            for r in rows
        ]


def insert_weekly_drift(
    week_start: str,
    week_end: str,
    channel_id: str,
    avg_theological_density: float,
    grace_vs_effort_zscore: float,
    hope_vs_fear_zscore: float,
    doctrine_vs_experience_zscore: float,
    scripture_vs_story_zscore: float,
    sample_size: int,
    report_json: str,
) -> None:
    """Insert weekly drift report"""
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO weekly_drift_reports
                (week_start, week_end, channel_id, avg_theological_density,
                 grace_vs_effort_zscore, hope_vs_fear_zscore, doctrine_vs_experience_zscore,
                 scripture_vs_story_zscore, sample_size, report_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (week_start, week_end, channel_id, avg_theological_density,
             grace_vs_effort_zscore, hope_vs_fear_zscore, doctrine_vs_experience_zscore,
             scripture_vs_story_zscore, sample_size, report_json),
        )
        conn.commit()


def insert_assembly_script(
    week_start: str,
    week_end: str,
    script_text: str,
    avatar_assignments_json: str,
    source_video_ids: str,
) -> None:
    """Insert assembly script"""
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO assembly_scripts
                (week_start, week_end, script_text, avatar_assignments_json, source_video_ids)
            VALUES (?, ?, ?, ?, ?)
            """,
            (week_start, week_end, script_text, avatar_assignments_json, source_video_ids),
        )
        conn.commit()