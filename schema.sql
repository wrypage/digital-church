CREATE TABLE IF NOT EXISTS channels (
    channel_id TEXT PRIMARY KEY,
    channel_name TEXT,
    source_url TEXT UNIQUE,
    resolved_via TEXT,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    active INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS videos (
    video_id TEXT PRIMARY KEY,
    channel_id TEXT NOT NULL,
    title TEXT,
    published_at TIMESTAMP,
    duration_seconds INTEGER,
    status TEXT DEFAULT 'discovered',
    error_message TEXT,
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
);

CREATE TABLE IF NOT EXISTS transcripts (
    transcript_id INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id TEXT NOT NULL UNIQUE,
    full_text TEXT,
    segments_json TEXT,
    language TEXT,
    word_count INTEGER,
    transcript_provider TEXT DEFAULT 'openai_api',
    transcript_model TEXT,
    transcript_version TEXT DEFAULT 'v5.2',
    transcribed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (video_id) REFERENCES videos(video_id)
);

CREATE TABLE IF NOT EXISTS brain_results (
    result_id INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id TEXT NOT NULL,
    theological_density REAL,
    grace_vs_effort REAL,
    hope_vs_fear REAL,
    doctrine_vs_experience REAL,
    scripture_vs_story REAL,
    top_categories TEXT,
    raw_scores_json TEXT,
    analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (video_id) REFERENCES videos(video_id)
);

CREATE TABLE IF NOT EXISTS weekly_drift_reports (
    report_id INTEGER PRIMARY KEY AUTOINCREMENT,
    week_start DATE NOT NULL,
    week_end DATE NOT NULL,
    channel_id TEXT,
    avg_theological_density REAL,
    grace_vs_effort_zscore REAL,
    hope_vs_fear_zscore REAL,
    doctrine_vs_experience_zscore REAL,
    scripture_vs_story_zscore REAL,
    sample_size INTEGER,
    report_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS assembly_scripts (
    script_id INTEGER PRIMARY KEY AUTOINCREMENT,
    week_start DATE,
    week_end DATE,
    script_text TEXT,
    avatar_assignments_json TEXT,
    source_video_ids TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_type TEXT NOT NULL,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP,
    status TEXT DEFAULT 'running',
    videos_processed INTEGER DEFAULT 0,
    minutes_processed REAL DEFAULT 0,
    notes TEXT
);
