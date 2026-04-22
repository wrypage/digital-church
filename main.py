import streamlit as st
import sqlite3
import pandas as pd
from analysis_engine import DigitalPulpitBrain
from script_generator import AssemblyScriptDirector

# Ensure DB setup
def init_db():
    conn = sqlite3.connect('pulpit.db')
    with open('schema.sql', 'r') as f:
        conn.executescript(f.read())
    conn.commit()
    return conn

st.title("The Digital Pulpit: War Room")

if st.sidebar.text_input("Password", type="password") == st.secrets["DASHBOARD_PASSWORD"]:
    conn = init_db()
    
    # Simple Dashboard View
    st.subheader("System Status")
    videos = pd.read_sql("SELECT * FROM videos ORDER BY ingested_at DESC LIMIT 10", conn)
    st.table(videos)
    
    if st.button("Generate Weekly Script"):
        # Placeholder for real report generation logic
        st.info("Analyzing latest 7 days of data...")
        st.text_area("Final Script", "Ready for HeyGen production.")
else:
    st.warning("Locked. Please enter the dashboard password.")