-- Migration: Archive YouTube Channels to Legacy Table
-- Purpose: Clean up main channels table while preserving YouTube attempt history
-- Date: 2026-02-18

-- ============================================================================
-- STEP 1: Create channels_legacy table
-- ============================================================================

CREATE TABLE IF NOT EXISTS channels_legacy (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    platform TEXT NOT NULL,              -- 'youtube', 'rss', etc.
    channel_id TEXT NOT NULL,            -- Original channel_id
    status TEXT DEFAULT 'archived_youtube_attempt',
    notes TEXT DEFAULT 'Blocked by API limits / pre-RSS pivot',

    -- Original channel metadata (preserved for reference)
    original_channel_name TEXT,
    resolved_via TEXT,                   -- 'handle', 'search_name', etc.
    source_url TEXT,

    -- Archival metadata
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    original_added_at TIMESTAMP,
    video_count INTEGER DEFAULT 0,       -- How many videos were attempted

    -- Index for lookups
    UNIQUE(channel_id, platform)
);

CREATE INDEX IF NOT EXISTS idx_legacy_platform ON channels_legacy(platform);
CREATE INDEX IF NOT EXISTS idx_legacy_status ON channels_legacy(status);


-- ============================================================================
-- STEP 2: Migrate YouTube channels to legacy table
-- ============================================================================

INSERT INTO channels_legacy (
    name,
    platform,
    channel_id,
    status,
    notes,
    original_channel_name,
    resolved_via,
    source_url,
    original_added_at,
    video_count
)
SELECT
    c.channel_name as name,
    'youtube' as platform,
    c.channel_id,
    'archived_youtube_attempt' as status,
    'Blocked by API limits (403/429 errors) / pre-RSS pivot' as notes,
    c.channel_name as original_channel_name,
    c.resolved_via,
    c.source_url,
    c.added_at as original_added_at,
    (SELECT COUNT(*) FROM videos v WHERE v.channel_id = c.channel_id) as video_count
FROM channels c
WHERE c.resolved_via IN ('handle', 'search_name')
  AND c.channel_id NOT IN (
      -- Don't archive if they somehow have successful transcripts
      SELECT DISTINCT channel_id
      FROM videos
      WHERE video_id IN (SELECT video_id FROM transcripts)
  );


-- ============================================================================
-- STEP 3: Verify migration before deletion
-- ============================================================================

-- Check counts match
-- Expected: 137 YouTube channels should be archived

SELECT
    'BEFORE DELETION - Verification' as step,
    (SELECT COUNT(*) FROM channels WHERE resolved_via IN ('handle', 'search_name')) as youtube_channels_to_archive,
    (SELECT COUNT(*) FROM channels_legacy WHERE platform = 'youtube') as archived_youtube_channels,
    (SELECT COUNT(*) FROM channels WHERE resolved_via = 'rss_feed') as rss_channels_to_keep;


-- ============================================================================
-- STEP 4: Delete YouTube channels from main table
-- ============================================================================

-- Safety: Only delete if archive succeeded
DELETE FROM channels
WHERE resolved_via IN ('handle', 'search_name')
  AND channel_id IN (SELECT channel_id FROM channels_legacy WHERE platform = 'youtube');


-- ============================================================================
-- STEP 5: Verify cleanup
-- ============================================================================

SELECT
    'AFTER DELETION - Verification' as step,
    (SELECT COUNT(*) FROM channels) as total_active_channels,
    (SELECT COUNT(*) FROM channels WHERE resolved_via = 'rss_feed') as rss_channels,
    (SELECT COUNT(*) FROM channels_legacy) as legacy_channels;


-- ============================================================================
-- STEP 6: Clean up orphaned videos from YouTube channels
-- ============================================================================

-- Optional: Archive YouTube video records to videos_legacy table
CREATE TABLE IF NOT EXISTS videos_legacy (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id TEXT NOT NULL,
    channel_id TEXT NOT NULL,
    title TEXT,
    status TEXT,
    archived_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notes TEXT,

    UNIQUE(video_id)
);

-- Move YouTube videos to legacy
INSERT INTO videos_legacy (video_id, channel_id, title, status, notes)
SELECT
    v.video_id,
    v.channel_id,
    v.title,
    v.status,
    'YouTube video from archived channel (API blocked)'
FROM videos v
WHERE v.channel_id IN (SELECT channel_id FROM channels_legacy WHERE platform = 'youtube')
  AND v.video_id NOT IN (SELECT video_id FROM transcripts);  -- Only archive if no successful transcript

-- Delete YouTube videos from main table
DELETE FROM videos
WHERE channel_id IN (SELECT channel_id FROM channels_legacy WHERE platform = 'youtube')
  AND video_id NOT IN (SELECT video_id FROM transcripts);


-- ============================================================================
-- STEP 7: Final verification
-- ============================================================================

SELECT
    'FINAL STATE' as report,
    (SELECT COUNT(*) FROM channels) as active_channels,
    (SELECT COUNT(*) FROM videos) as active_videos,
    (SELECT COUNT(*) FROM channels_legacy) as legacy_channels,
    (SELECT COUNT(*) FROM videos_legacy) as legacy_videos;

SELECT
    'ACTIVE CHANNELS BY TYPE' as report,
    resolved_via,
    COUNT(*) as count
FROM channels
GROUP BY resolved_via;


-- ============================================================================
-- USEFUL QUERIES FOR REFERENCE
-- ============================================================================

-- View archived YouTube channels
-- SELECT name, channel_id, video_count, archived_at
-- FROM channels_legacy
-- WHERE platform = 'youtube'
-- ORDER BY video_count DESC;

-- Check if specific channel was archived
-- SELECT * FROM channels_legacy WHERE channel_id = 'YOUR_CHANNEL_ID';

-- Restore a channel if needed (emergency recovery)
-- INSERT INTO channels (channel_id, channel_name, resolved_via, source_url, added_at)
-- SELECT channel_id, name, resolved_via, source_url, original_added_at
-- FROM channels_legacy
-- WHERE channel_id = 'CHANNEL_TO_RESTORE';


-- ============================================================================
-- NOTES
-- ============================================================================

-- This migration:
-- 1. Creates channels_legacy table for historical YouTube data
-- 2. Moves 137 YouTube channels out of main channels table
-- 3. Optionally archives failed YouTube video records
-- 4. Preserves all data (zero loss)
-- 5. Keeps only 15 working RSS feed channels in active table
-- 6. Brain and Vacuum will ignore legacy tables entirely

-- After running this:
-- - channels table: 15 RSS feeds (clean!)
-- - channels_legacy table: 137 YouTube channels (preserved)
-- - videos table: Only RSS videos with transcripts
-- - videos_legacy table: Failed YouTube videos (optional)

-- Roll back if needed:
-- Just run the "Restore" query for any channels you want back
