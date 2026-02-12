import streamlit as st
import os
import sys
import logging
import threading
import pandas as pd
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")

from engine import db
from engine.config import DASHBOARD_PASSWORD

db.init_db()

st.set_page_config(page_title="Digital Pulpit", page_icon="📡", layout="wide")


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


def run_vacuum_background():
    from engine.vacuum import run_vacuum
    st.session_state.vacuum_running = True
    try:
        run_vacuum()
    except Exception as e:
        logging.getLogger("digital_pulpit").error(f"Vacuum error: {e}")
    finally:
        st.session_state.vacuum_running = False


def run_brain_background():
    from engine.brain import run_brain
    try:
        run_brain()
    except Exception as e:
        logging.getLogger("digital_pulpit").error(f"Brain error: {e}")


def run_assembly_background():
    from engine.assembly import run_assembly
    try:
        run_assembly()
    except Exception as e:
        logging.getLogger("digital_pulpit").error(f"Assembly error: {e}")


if not check_password():
    st.stop()

st.title("Digital Pulpit Intelligence Engine")
st.caption("v5.2 — Theological Intelligence from Global Sermon Data")

tab_status, tab_videos, tab_transcripts, tab_brain, tab_scripts, tab_controls = st.tabs(
    ["Status", "Videos", "Transcripts", "Brain & Drift", "Scripts", "Controls"]
)

with tab_status:
    st.header("System Status")
    stats = db.get_db_stats()

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
    runs = db.get_recent_runs(10)
    if runs:
        runs_df = pd.DataFrame(runs)
        display_cols = [c for c in ["run_id", "run_type", "status", "videos_processed", "minutes_processed", "started_at", "notes"] if c in runs_df.columns]
        st.dataframe(runs_df[display_cols], use_container_width=True)
    else:
        st.info("No runs recorded yet.")

with tab_videos:
    st.header("Recent Videos")
    videos = db.get_recent_videos(50)
    if videos:
        vdf = pd.DataFrame(videos)
        display_cols = [c for c in ["video_id", "channel_name", "title", "status", "duration_seconds", "published_at"] if c in vdf.columns]
        st.dataframe(vdf[display_cols], use_container_width=True)
    else:
        st.info("No videos discovered yet. Run the Vacuum to start ingesting.")

with tab_transcripts:
    st.header("Recent Transcripts")
    transcripts = db.get_recent_transcripts(20)
    if transcripts:
        tdf = pd.DataFrame(transcripts)
        display_cols = [c for c in ["video_id", "channel_name", "title", "word_count", "language", "transcript_model", "transcribed_at"] if c in tdf.columns]
        st.dataframe(tdf[display_cols], use_container_width=True)

        st.subheader("Transcript Preview")
        selected = st.selectbox("Select transcript:", [f"{t['title']} ({t['video_id']})" for t in transcripts])
        if selected:
            vid = selected.split("(")[-1].rstrip(")")
            for t in transcripts:
                if t["video_id"] == vid:
                    st.text_area("Full Text", t.get("full_text", "")[:3000], height=300)
                    break
    else:
        st.info("No transcripts yet. Run the Vacuum to transcribe sermons.")

with tab_brain:
    st.header("Brain Analysis & Drift")

    results = db.get_all_brain_results()
    if results:
        rdf = pd.DataFrame(results)
        st.subheader("Theological Density")
        display_cols = [c for c in ["video_id", "channel_name", "title", "theological_density", "grace_vs_effort", "hope_vs_fear", "doctrine_vs_experience", "scripture_vs_story"] if c in rdf.columns]
        st.dataframe(rdf[display_cols], use_container_width=True)

        st.subheader("Drift Axes (Latest Results)")
        if len(rdf) > 0:
            chart_data = rdf[["grace_vs_effort", "hope_vs_fear", "doctrine_vs_experience", "scripture_vs_story"]].tail(20)
            st.line_chart(chart_data)
    else:
        st.info("No analysis results. Run the Brain after transcribing sermons.")

    st.subheader("Weekly Drift Reports")
    drift = db.get_weekly_drift_reports(10)
    if drift:
        ddf = pd.DataFrame(drift)
        display_cols = [c for c in ["week_start", "week_end", "channel_id", "avg_theological_density", "grace_vs_effort_zscore", "hope_vs_fear_zscore", "sample_size"] if c in ddf.columns]
        st.dataframe(ddf[display_cols], use_container_width=True)

with tab_scripts:
    st.header("Assembly Scripts")
    scripts = db.get_assembly_scripts(10)
    if scripts:
        for s in scripts:
            with st.expander(f"Script: {s['week_start']} to {s['week_end']} (ID: {s['script_id']})"):
                st.markdown(s.get("script_text", ""))
    else:
        st.info("No scripts generated yet. Run the Assembly to create avatar scripts.")

with tab_controls:
    st.header("Pipeline Controls")
    st.markdown("---")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("1. The Vacuum")
        st.caption("Ingest channels, discover videos, download audio, transcribe")
        vacuum_running = st.session_state.get("vacuum_running", False)
        if st.button("Run Vacuum Now", disabled=vacuum_running, use_container_width=True):
            with st.spinner("Running Vacuum pipeline... This may take several minutes."):
                from engine.vacuum import run_vacuum
                run_id = run_vacuum()
                st.success(f"Vacuum complete! Run ID: {run_id}")
                st.rerun()

    with col2:
        st.subheader("2. The Brain")
        st.caption("Analyze transcripts, compute density & drift")
        if st.button("Run Brain Now", use_container_width=True):
            with st.spinner("Running Brain analysis..."):
                from engine.brain import run_brain
                run_id = run_brain()
                st.success(f"Brain complete! Run ID: {run_id}")
                st.rerun()

    with col3:
        st.subheader("3. The Assembly")
        st.caption("Generate weekly avatar scripts")
        if st.button("Generate Weekly Script Now", use_container_width=True):
            with st.spinner("Generating script..."):
                from engine.assembly import run_assembly
                run_id = run_assembly()
                st.success(f"Assembly complete! Run ID: {run_id}")
                st.rerun()

    st.markdown("---")
    st.subheader("Full Pipeline")
    if st.button("Run Full Pipeline (Vacuum + Brain + Assembly)", use_container_width=True):
        with st.spinner("Running full pipeline..."):
            from engine.vacuum import run_vacuum
            from engine.brain import run_brain
            from engine.assembly import run_assembly
            v_id = run_vacuum()
            b_id = run_brain()
            a_id = run_assembly()
            st.success(f"Full pipeline complete! Vacuum: {v_id}, Brain: {b_id}, Assembly: {a_id}")
            st.rerun()

    st.markdown("---")
    st.subheader("Environment")
    env_status = {
        "YOUTUBE_API_KEY": "Set" if os.environ.get("YOUTUBE_API_KEY") else "NOT SET",
        "OPENAI_API_KEY": "Set" if os.environ.get("OPENAI_API_KEY") else "NOT SET",
        "DASHBOARD_PASSWORD": "Set" if os.environ.get("DASHBOARD_PASSWORD") else "NOT SET (open access)",
        "MAX_VIDEOS_PER_RUN": os.environ.get("MAX_VIDEOS_PER_RUN", "120"),
        "MAX_MINUTES_PER_RUN": os.environ.get("MAX_MINUTES_PER_RUN", "180"),
    }
    st.json(env_status)
