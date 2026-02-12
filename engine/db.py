import sqlite3
import os
import logging
from engine.config import DATABASE_PATH

logger = logging.getLogger("digital_pulpit")


def get_connection():
    os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    conn = get_connection()
    schema_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "schema.sql")
    with open(schema_path, "r") as f:
        conn.executescript(f.read())
    conn.commit()
    conn.close()
    logger.info("Database initialized")


def upsert_channel(channel_id, channel_name, source_url, resolved_via):
    conn = get_connection()
    conn.execute(
        """INSERT INTO channels (channel_id, channel_name, source_url, resolved_via)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(channel_id) DO UPDATE SET
             channel_name=excluded.channel_name,
             source_url=excluded.source_url""",
        (channel_id, channel_name, source_url, resolved_via),
    )
    conn.commit()
    conn.close()


def upsert_video(video_id, channel_id, title, published_at, duration_seconds, status="discovered"):
    conn = get_connection()
    conn.execute(
        """INSERT INTO videos (video_id, channel_id, title, published_at, duration_seconds, status)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(video_id) DO NOTHING""",
        (video_id, channel_id, title, published_at, duration_seconds, status),
    )
    conn.commit()
    conn.close()


def update_video_status(video_id, status, error_message=None):
    conn = get_connection()
    conn.execute(
        """UPDATE videos SET status=?, error_message=?, updated_at=CURRENT_TIMESTAMP
           WHERE video_id=?""",
        (status, error_message, video_id),
    )
    conn.commit()
    conn.close()


def get_videos_by_status(status, limit=None):
    conn = get_connection()
    query = "SELECT * FROM videos WHERE status=? ORDER BY published_at DESC"
    if limit:
        query += f" LIMIT {limit}"
    rows = conn.execute(query, (status,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def insert_transcript(video_id, full_text, segments_json, language, word_count, model):
    conn = get_connection()
    conn.execute(
        """INSERT INTO transcripts (video_id, full_text, segments_json, language, word_count, transcript_model)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(video_id) DO UPDATE SET
             full_text=excluded.full_text,
             segments_json=excluded.segments_json,
             language=excluded.language,
             word_count=excluded.word_count,
             transcript_model=excluded.transcript_model,
             transcribed_at=CURRENT_TIMESTAMP""",
        (video_id, full_text, segments_json, language, word_count, model),
    )
    conn.commit()
    conn.close()


def get_transcript(video_id):
    conn = get_connection()
    row = conn.execute("SELECT * FROM transcripts WHERE video_id=?", (video_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def insert_brain_result(video_id, density, grace_effort, hope_fear, doctrine_exp, scripture_story, top_cats, raw_json):
    conn = get_connection()
    conn.execute(
        """INSERT INTO brain_results (video_id, theological_density, grace_vs_effort,
           hope_vs_fear, doctrine_vs_experience, scripture_vs_story, top_categories, raw_scores_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (video_id, density, grace_effort, hope_fear, doctrine_exp, scripture_story, top_cats, raw_json),
    )
    conn.commit()
    conn.close()


def insert_weekly_drift(week_start, week_end, channel_id, avg_density,
                        grace_z, hope_z, doctrine_z, scripture_z, sample_size, report_json):
    conn = get_connection()
    conn.execute(
        """INSERT INTO weekly_drift_reports (week_start, week_end, channel_id, avg_theological_density,
           grace_vs_effort_zscore, hope_vs_fear_zscore, doctrine_vs_experience_zscore,
           scripture_vs_story_zscore, sample_size, report_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (week_start, week_end, channel_id, avg_density, grace_z, hope_z, doctrine_z, scripture_z, sample_size, report_json),
    )
    conn.commit()
    conn.close()


def insert_assembly_script(week_start, week_end, script_text, avatar_json, source_ids):
    conn = get_connection()
    conn.execute(
        """INSERT INTO assembly_scripts (week_start, week_end, script_text, avatar_assignments_json, source_video_ids)
           VALUES (?, ?, ?, ?, ?)""",
        (week_start, week_end, script_text, avatar_json, source_ids),
    )
    conn.commit()
    conn.close()


def create_run(run_type):
    conn = get_connection()
    cursor = conn.execute("INSERT INTO runs (run_type) VALUES (?)", (run_type,))
    run_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return run_id


def finish_run(run_id, status, videos_processed, minutes_processed, notes=""):
    conn = get_connection()
    conn.execute(
        """UPDATE runs SET finished_at=CURRENT_TIMESTAMP, status=?, videos_processed=?,
           minutes_processed=?, notes=? WHERE run_id=?""",
        (status, videos_processed, minutes_processed, notes, run_id),
    )
    conn.commit()
    conn.close()


def get_recent_runs(limit=20):
    conn = get_connection()
    rows = conn.execute("SELECT * FROM runs ORDER BY started_at DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_recent_videos(limit=50):
    conn = get_connection()
    rows = conn.execute(
        """SELECT v.*, c.channel_name FROM videos v
           LEFT JOIN channels c ON v.channel_id = c.channel_id
           ORDER BY v.discovered_at DESC LIMIT ?""",
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_recent_transcripts(limit=20):
    conn = get_connection()
    rows = conn.execute(
        """SELECT t.*, v.title, c.channel_name FROM transcripts t
           JOIN videos v ON t.video_id = v.video_id
           LEFT JOIN channels c ON v.channel_id = c.channel_id
           ORDER BY t.transcribed_at DESC LIMIT ?""",
        (limit,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_db_stats():
    conn = get_connection()
    stats = {}
    for table in ["channels", "videos", "transcripts", "brain_results", "weekly_drift_reports", "assembly_scripts", "runs"]:
        row = conn.execute(f"SELECT COUNT(*) as cnt FROM {table}").fetchone()
        stats[table] = row["cnt"]
    video_statuses = conn.execute(
        "SELECT status, COUNT(*) as cnt FROM videos GROUP BY status"
    ).fetchall()
    stats["video_statuses"] = {r["status"]: r["cnt"] for r in video_statuses}
    conn.close()
    return stats


def get_weekly_drift_reports(limit=10):
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM weekly_drift_reports ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_assembly_scripts(limit=10):
    conn = get_connection()
    rows = conn.execute(
        "SELECT * FROM assembly_scripts ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_all_brain_results():
    conn = get_connection()
    rows = conn.execute(
        """SELECT br.*, v.title, v.channel_id, c.channel_name
           FROM brain_results br
           JOIN videos v ON br.video_id = v.video_id
           LEFT JOIN channels c ON v.channel_id = c.channel_id
           ORDER BY br.analyzed_at DESC"""
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_transcribed_videos_without_analysis():
    conn = get_connection()
    rows = conn.execute(
        """SELECT v.* FROM videos v
           WHERE v.status IN ('transcribed', 'queued_for_brain')
           AND v.video_id NOT IN (SELECT video_id FROM brain_results)
           ORDER BY v.published_at DESC"""
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
