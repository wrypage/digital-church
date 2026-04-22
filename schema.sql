-- 1. Channels Table: Persistent Resolution Cache
CREATE TABLE IF NOT EXISTS channels (
    channel_id TEXT PRIMARY KEY,
    channel_name TEXT,
    channel_url TEXT UNIQUE,
    resolved_from TEXT,
    last_checked_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    status TEXT DEFAULT 'active',
    error_message TEXT
);

-- 2. Videos Table: Pipeline State Management
CREATE TABLE IF NOT EXISTS videos (
    video_id TEXT PRIMARY KEY,
    channel_id TEXT,
    title TEXT,
    published_at DATETIME,
    duration_seconds INTEGER,
    video_url TEXT,
    ingested_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    status TEXT NOT NULL DEFAULT 'discovered' CHECK(status IN ('discovered', 'audio_downloaded', 'transcribed', 'queued_for_brain', 'failed')),
    error_message TEXT,
    FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
);

-- 3. Transcripts Table: The Core Data with Cascade Delete
CREATE TABLE IF NOT EXISTS transcripts (
    video_id TEXT PRIMARY KEY,
    full_text TEXT,
    segments_json TEXT, 
    language TEXT,
    word_count INTEGER,
    theological_density REAL,
    transcript_provider TEXT,
    transcript_model TEXT,
    transcript_version TEXT,
    transcribed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (video_id) REFERENCES videos(video_id) ON DELETE CASCADE
);

-- 4. Quote Units Table: Strategic Content Extraction (Hardened)
CREATE TABLE IF NOT EXISTS quote_units (
    quote_id INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id TEXT NOT NULL,
    start_seconds REAL,
    end_seconds REAL,
    text TEXT NOT NULL,
    layers_json TEXT,
    tags_json TEXT,
    tone TEXT,
    base_quote_score REAL,
    character_scores_json TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (video_id) REFERENCES videos(video_id) ON DELETE CASCADE,
    UNIQUE(video_id, start_seconds, end_seconds)
);

-- 5. Weekly Metrics & Drift (Clarified Naming)
CREATE TABLE IF NOT EXISTS weekly_drift_reports (
    week_start_date DATE PRIMARY KEY,
    primary_metric TEXT,
    drift_metrics_json TEXT, 
    heartbeat_report_json TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 6. Runs Table: The Audit Log
CREATE TABLE IF NOT EXISTS runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    ended_at DATETIME,
    videos_discovered INTEGER,
    videos_transcribed INTEGER,
    failures_count INTEGER,
    notes TEXT
);

-- 7. Run-Video Join Table: Granular Debugging (Optional but Powerful)
CREATE TABLE IF NOT EXISTS run_videos (
    run_id INTEGER NOT NULL,
    video_id TEXT NOT NULL,
    stage TEXT, -- e.g., 'transcription', 'brain_analysis'
    status TEXT, -- 'success', 'failed'
    error_message TEXT,
    PRIMARY KEY (run_id, video_id),
    FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE,
    FOREIGN KEY (video_id) REFERENCES videos(video_id) ON DELETE CASCADE
);

-- 8. Performance Indexes
CREATE INDEX IF NOT EXISTS idx_videos_channel ON videos(channel_id);
CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(status);
CREATE INDEX IF NOT EXISTS idx_videos_published ON videos(published_at);
CREATE INDEX IF NOT EXISTS idx_quotes_video_score ON quote_units(video_id, base_quote_score);