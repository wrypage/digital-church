import os
import logging
import sqlite3
import pandas as pd
import streamlit as st

from engine import db
from engine.config import DASHBOARD_PASSWORD, DATABASE_PATH
from engine.pipeline import run_vacuum, run_brain, run_assembly, run_all

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")

# Initialize DB if the function exists (older db.py had init_db; newer refactors might not)
if hasattr(db, "init_db"):
    try:
        db.init_db()
    except Exception as e:
        logging.getLogger("digital_pulpit").warning(
            f"db.init_db() failed: {e}")

st.set_page_config(page_title="Digital Pulpit", page_icon=None, layout="wide")


# -------------------------
# SQLite direct-read helpers
# -------------------------
class _ReadOnlyConnection:
    """Context manager wrapper for readonly sqlite3 connections."""
    def __init__(self):
        self.conn = None
    
    def __enter__(self):
        self.conn = sqlite3.connect(DATABASE_PATH, timeout=15)
        self.conn.row_factory = sqlite3.Row
        try:
            self.conn.execute("PRAGMA busy_timeout=5000")
        except Exception:
            pass
        return self.conn
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
        return False


def _connect_readonly():
    return _ReadOnlyConnection()


def _table_cols(conn, table: str):
    return [
        r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()
    ]


def _safe_has(conn, table: str, col: str) -> bool:
    return col in _table_cols(conn, table)


def _youtube_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"


def _get_recent_videos(limit: int = 100):
    with _connect_readonly() as conn:
        cols = _table_cols(conn, "videos")

        # We’ll select only what exists (schema-adaptive)
        wanted = [
            "video_id", "channel_id", "title", "published_at", "duration_seconds",
            "status", "error_message", "discovered_at", "updated_at"
        ]
        select_cols = [c for c in wanted if c in cols]
        if not select_cols:
            return []

        sql = f"SELECT {', '.join(select_cols)} FROM videos ORDER BY COALESCE(updated_at, discovered_at, published_at, rowid) DESC LIMIT ?"
        rows = conn.execute(sql, (limit, )).fetchall()
        return [dict(r) for r in rows]


def _get_recent_transcripts(limit: int = 50):
    with _connect_readonly() as conn:
        tcols = _table_cols(conn, "transcripts")

        # transcripts columns vary; we handle common names
        # Always require video_id
        if "video_id" not in tcols:
            return []

        # Try to include title by joining videos if possible
        vcols = _table_cols(conn, "videos") if _safe_has(conn, "videos",
                                                         "video_id") else []
        has_videos = "video_id" in vcols

        # Choose transcript text column name (your schema uses transcript_text)
        text_col = "transcript_text" if "transcript_text" in tcols else None

        # Word count / language / model columns may vary
        wc_col = "word_count" if "word_count" in tcols else None
        lang_col = "language" if "language" in tcols else None
        model_col = "model" if "model" in tcols else None

        base_cols = ["video_id"]
        if wc_col:
            base_cols.append(wc_col)
        if lang_col:
            base_cols.append(lang_col)
        if model_col:
            base_cols.append(model_col)

        if has_videos:
            select = ["t.video_id"]
            if wc_col:
                select.append(f"t.{wc_col} AS word_count")
            if lang_col:
                select.append(f"t.{lang_col} AS language")
            if model_col:
                select.append(f"t.{model_col} AS transcript_model")
            if "title" in vcols:
                select.append("v.title AS title")
            if "channel_id" in vcols:
                select.append("v.channel_id AS channel_id")
            if "status" in vcols:
                select.append("v.status AS video_status")
            sql = f"""
                SELECT {', '.join(select)}
                FROM transcripts t
                LEFT JOIN videos v ON v.video_id = t.video_id
                ORDER BY t.rowid DESC
                LIMIT ?
            """
            rows = conn.execute(sql, (limit, )).fetchall()
            return [dict(r) for r in rows]

        # No videos table join
        sql = f"SELECT {', '.join(base_cols)} FROM transcripts ORDER BY rowid DESC LIMIT ?"
        rows = conn.execute(sql, (limit, )).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            # normalize names
            if wc_col and wc_col != "word_count":
                d["word_count"] = d.get(wc_col)
            if model_col and model_col != "transcript_model":
                d["transcript_model"] = d.get(model_col)
            out.append(d)
        return out


def _get_transcript_text(video_id: str) -> str:
    with _connect_readonly() as conn:
        tcols = _table_cols(conn, "transcripts")
        if "video_id" not in tcols:
            return ""

        text_col = "transcript_text" if "transcript_text" in tcols else None
        if not text_col:
            return ""

        row = conn.execute(
            f"SELECT {text_col} FROM transcripts WHERE video_id = ? LIMIT 1",
            (video_id, ),
        ).fetchone()
        return (row[text_col] if row and row[text_col] else "") or ""


def _get_video_row(video_id: str):
    with _connect_readonly() as conn:
        vcols = _table_cols(conn, "videos")
        if "video_id" not in vcols:
            return None

        wanted = [
            "video_id", "channel_id", "title", "published_at", "duration_seconds",
            "status", "error_message", "discovered_at", "updated_at"
        ]
        select_cols = [c for c in wanted if c in vcols]
        sql = f"SELECT {', '.join(select_cols)} FROM videos WHERE video_id = ? LIMIT 1"
        row = conn.execute(sql, (video_id, )).fetchone()
        return dict(row) if row else None


    # -------------------------
    # Auth
    # -------------------------
def check_password():
    if not DASHBOARD_PASSWORD:
        return True

    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if st.session_state.authenticated:
        return True

    with st.container():
        st.title("Digital Pulpit Intelligence Engine")
        st.markdown("---")
        pwd = st.text_input("Enter dashboard password:", type="password")
        if st.button("Login"):
            if pwd == DASHBOARD_PASSWORD:
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.error("Incorrect password")
    return False


if not check_password():
    st.stop()

# -------------------------
# UI
# -------------------------
st.title("Digital Pulpit Intelligence Engine")
st.caption("v5.2 — Theological Intelligence from Global Sermon Data")

tab_status, tab_videos, tab_transcripts, tab_brain, tab_scripts, tab_controls = st.tabs(
    [
        "Status", "Videos", "Transcripts", "Brain & Drift", "Scripts",
        "Controls"
    ])

with tab_status:
    st.header("System Status")
    stats = db.get_db_stats() if hasattr(db, "get_db_stats") else {}

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Channels", stats.get("channels", 0))
    col2.metric("Videos", stats.get("videos", 0))
    col3.metric("Transcripts", stats.get("transcripts", 0))
    col4.metric("Brain Results", stats.get("brain_results", 0))

    st.subheader("Video Status Breakdown")
    statuses = stats.get("video_statuses", {})
    if statuses:
        df = pd.DataFrame(list(statuses.items()), columns=["Status", "Count"])
        st.bar_chart(df.set_index("Status"))
    else:
        st.info("No videos ingested yet.")

    st.subheader("Recent Runs")
    runs = db.get_recent_runs(10) if hasattr(db, "get_recent_runs") else []
    if runs:
        runs_df = pd.DataFrame(runs)
        display_cols = [
            c for c in [
                "run_id", "run_type", "status", "videos_processed",
                "minutes_processed", "started_at", "finished_at", "notes"
            ] if c in runs_df.columns
        ]
        st.dataframe(runs_df[display_cols], use_container_width=True)
    else:
        st.info("No runs recorded yet.")

    with st.expander("Database Path / Debug"):
        st.code(f"DATABASE_PATH = {DATABASE_PATH}")

with tab_videos:
    st.header("Videos")
    st.caption(
        "Browse discovered videos. Select one to view status + transcript (if available)."
    )

    # Use direct read for stability
    videos = _get_recent_videos(200)
    if not videos:
        st.info("No videos discovered yet. Run the Vacuum to start ingesting.")
    else:
        vdf = pd.DataFrame(videos)

        # Filters
        left, right = st.columns([1, 2])
        with left:
            statuses = sorted([
                s for s in vdf.get("status", pd.Series(
                    dtype=str)).dropna().unique().tolist()
            ]) if "status" in vdf.columns else []
            status_filter = st.multiselect("Filter by status",
                                           options=statuses,
                                           default=statuses)
        with right:
            q = st.text_input("Search title", value="")

        filtered = vdf
        if "status" in filtered.columns and status_filter:
            filtered = filtered[filtered["status"].isin(status_filter)]
        if q and "title" in filtered.columns:
            filtered = filtered[filtered["title"].fillna("").str.contains(
                q, case=False)]

        show_cols = [
            c for c in [
                "video_id", "title", "status", "duration_seconds",
                "published_at", "channel_id", "error_message"
            ] if c in filtered.columns
        ]
        st.dataframe(filtered[show_cols], use_container_width=True, height=320)

        st.subheader("Video Viewer")
        options = []
        for _, row in filtered.head(200).iterrows():
            title = row.get("title") or "(no title)"
            vid = row.get("video_id")
            if vid:
                options.append(f"{title} ({vid})")

        selected = st.selectbox("Select a video",
                                options=options,
                                index=0 if options else None)

        if selected:
            vid = selected.split("(")[-1].rstrip(")")
            video_row = _get_video_row(vid)
            if video_row:
                st.write("YouTube link:", _youtube_url(vid))
                meta_cols = st.columns(3)
                meta_cols[0].metric("Status", video_row.get("status", ""))
                meta_cols[1].metric("Duration (sec)",
                                    video_row.get("duration_seconds", 0) or 0)
                meta_cols[2].metric("Published",
                                    video_row.get("published_at", "") or "")

                if video_row.get("error_message"):
                    st.warning(f"Error: {video_row.get('error_message')}")

                t = _get_transcript_text(vid)
                if t:
                    st.markdown("Transcript")
                    st.text_area("Full transcript", t, height=420)
                else:
                    st.info(
                        "No transcript found for this video yet. Run Vacuum (or check if it was skipped)."
                    )

with tab_transcripts:
    st.header("Transcripts")
    st.caption("Browse transcripts and open the full text.")

    transcripts = _get_recent_transcripts(50)
    if not transcripts:
        st.info("No transcripts yet. Run the Vacuum to transcribe sermons.")
    else:
        tdf = pd.DataFrame(transcripts)
        show_cols = [
            c for c in [
                "video_id", "title", "word_count", "language",
                "transcript_model", "video_status", "channel_id"
            ] if c in tdf.columns
        ]
        st.dataframe(tdf[show_cols], use_container_width=True, height=320)

        st.subheader("Transcript Viewer")
        options = []
        for t in transcripts:
            title = t.get("title") or "(no title)"
            vid = t.get("video_id")
            if vid:
                options.append(f"{title} ({vid})")

        selected = st.selectbox("Select a transcript",
                                options=options,
                                index=0 if options else None)

        if selected:
            vid = selected.split("(")[-1].rstrip(")")
            text = _get_transcript_text(vid)
            st.write("YouTube link:", _youtube_url(vid))

            if text:
                st.text_area("Full transcript", text, height=520)
                st.download_button(
                    label="Download transcript as .txt",
                    data=text.encode("utf-8"),
                    file_name=f"{vid}.txt",
                    mime="text/plain",
                )
            else:
                st.warning(
                    "Transcript row exists, but transcript text column is empty or not found in schema."
                )

with tab_brain:
    st.header("Brain Analysis & Drift")

    results = db.get_all_brain_results() if hasattr(
        db, "get_all_brain_results") else []
    if results:
        rdf = pd.DataFrame(results)

        st.subheader("Theological Density")
        display_cols = [
            c for c in [
                "video_id", "channel_name", "title", "theological_density",
                "grace_vs_effort", "hope_vs_fear", "doctrine_vs_experience",
                "scripture_vs_story"
            ] if c in rdf.columns
        ]
        st.dataframe(rdf[display_cols], use_container_width=True)

        st.subheader("Drift Axes (Latest Results)")
        if len(rdf) > 0:
            chart_cols = [
                "grace_vs_effort", "hope_vs_fear", "doctrine_vs_experience",
                "scripture_vs_story"
            ]
            chart_cols = [c for c in chart_cols if c in rdf.columns]
            if chart_cols:
                chart_data = rdf[chart_cols].tail(20)
                st.line_chart(chart_data)
    else:
        st.info(
            "No analysis results. Run the Brain after transcribing sermons.")

    st.subheader("Weekly Drift Reports")
    drift = db.get_weekly_drift_reports(10) if hasattr(
        db, "get_weekly_drift_reports") else []
    if drift:
        ddf = pd.DataFrame(drift)
        display_cols = [
            c for c in [
                "week_start", "week_end", "channel_id",
                "avg_theological_density", "grace_vs_effort_zscore",
                "hope_vs_fear_zscore", "sample_size"
            ] if c in ddf.columns
        ]
        st.dataframe(ddf[display_cols], use_container_width=True)
    else:
        st.info("No drift reports yet.")

with tab_scripts:
    st.header("Assembly Scripts")
    scripts = db.get_assembly_scripts(10) if hasattr(
        db, "get_assembly_scripts") else []
    if scripts:
        for s in scripts:
            title = f"Script: {s.get('week_start','?')} to {s.get('week_end','?')} (ID: {s.get('script_id','?')})"
            with st.expander(title):
                st.markdown(s.get("script_text", ""))
    else:
        st.info(
            "No scripts generated yet. Run the Assembly to create avatar scripts."
        )

with tab_controls:
    st.header("Pipeline Controls")
    st.markdown("---")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("1. The Vacuum")
        st.caption(
            "Ingest channels, discover videos, download audio, transcribe")
        if st.button("Run Vacuum Now", use_container_width=True):
            with st.spinner(
                    "Running Vacuum pipeline... This may take several minutes."
            ):
                result = run_vacuum()
                if result.get("ok"):
                    st.success(
                        f"Vacuum complete! Run ID: {result.get('run_id')}")
                    if result.get("notes"):
                        st.info(f"Notes: {result.get('notes')}")
                else:
                    st.error(f"Vacuum failed: {result}")
                st.rerun()

    with col2:
        st.subheader("2. The Brain")
        st.caption("Analyze transcripts, compute density & drift")
        if st.button("Run Brain Now", use_container_width=True):
            with st.spinner("Running Brain analysis..."):
                result = run_brain()
                if result.get("ok"):
                    st.success(
                        f"Brain complete! Run ID: {result.get('run_id')}")
                else:
                    st.error(f"Brain failed: {result}")
                st.rerun()

    with col3:
        st.subheader("3. The Assembly")
        st.caption("Generate weekly avatar scripts")
        if st.button("Generate Weekly Script Now", use_container_width=True):
            with st.spinner("Generating script..."):
                result = run_assembly()
                if result.get("ok"):
                    st.success(
                        f"Assembly complete! Run ID: {result.get('run_id')}")
                else:
                    st.error(f"Assembly failed: {result}")
                st.rerun()

    st.markdown("---")
    st.subheader("Full Pipeline")
    if st.button("Run Full Pipeline (Vacuum + Brain + Assembly)",
                 use_container_width=True):
        with st.spinner("Running full pipeline..."):
            result = run_all()
            if result.get("ok"):
                st.success(
                    f"Full pipeline complete! "
                    f"Vacuum: {result.get('vacuum', {}).get('run_id')}, "
                    f"Brain: {result.get('brain', {}).get('run_id')}, "
                    f"Assembly: {result.get('assembly', {}).get('run_id')}")
            else:
                st.error(f"Full pipeline encountered an error: {result}")
            st.rerun()

    st.markdown("---")
    st.subheader("Environment")
    env_status = {
        "DATABASE_PATH":
        DATABASE_PATH,
        "YOUTUBE_API_KEY":
        "Set" if os.environ.get("YOUTUBE_API_KEY") else "NOT SET",
        "OPENAI_API_KEY":
        "Set" if os.environ.get("OPENAI_API_KEY") else "NOT SET",
        "DASHBOARD_PASSWORD":
        "Set"
        if os.environ.get("DASHBOARD_PASSWORD") else "NOT SET (open access)",
        "MAX_VIDEOS_PER_RUN":
        os.environ.get("MAX_VIDEOS_PER_RUN", "120"),
        "MAX_MINUTES_PER_RUN":
        os.environ.get("MAX_MINUTES_PER_RUN", "180"),
        "MIN_DURATION_SECONDS":
        os.environ.get("MIN_DURATION_SECONDS", "480"),
        "MAX_VIDEO_DURATION_SECONDS":
        os.environ.get("MAX_VIDEO_DURATION_SECONDS", "3600"),
    }
    st.json(env_status)
