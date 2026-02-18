#!/usr/bin/env python3
"""
Build transcript database from RSS feeds.

Pipeline:
1. Load all Tier 1 feeds from sermon_feeds.txt and sermon_feeds_2.txt
2. Select top 15 feeds (prioritize verse-by-verse exposition, 40+ min duration)
3. Download 5 most recent episodes from each feed
4. Transcribe using Whisper base model
5. Save everything to database
6. Generate progress report
"""

import os
import sys
import json
import logging
import hashlib
import re
import time
import sqlite3
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from pathlib import Path

import feedparser
import requests
import whisper

# Add engine to path
sys.path.insert(0, os.path.dirname(__file__))
from engine import db

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Config
TMP_AUDIO_DIR = Path(__file__).parent / "tmp_audio"
TMP_AUDIO_DIR.mkdir(exist_ok=True)
WHISPER_MODEL = "base"  # base model as requested
TARGET_EPISODES = 75  # 15 feeds * 5 episodes


def parse_duration_str(duration_str: str) -> Optional[int]:
    """Parse iTunes duration string to seconds."""
    if not duration_str or not str(duration_str).strip():
        return None

    duration_str = str(duration_str).strip()

    # Try parsing as integer (already in seconds)
    try:
        return int(duration_str)
    except ValueError:
        pass

    # Try parsing MM:SS or HH:MM:SS format
    parts = duration_str.split(':')
    try:
        if len(parts) == 2:  # MM:SS
            return int(parts[0]) * 60 + int(parts[1])
        elif len(parts) == 3:  # HH:MM:SS
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except (ValueError, IndexError):
        pass

    return None


def extract_tier1_feeds(report_file: str) -> List[Dict]:
    """Extract Tier 1 feeds from a sermon_feeds report file."""
    feeds = []

    with open(report_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # Find the TIER 1 section
    tier1_match = re.search(r'## TIER 1: EXCELLENT.*?\n\n(.*?)\n\n## TIER 2:', content, re.DOTALL)
    if not tier1_match:
        logger.warning(f"No TIER 1 section found in {report_file}")
        return feeds

    tier1_section = tier1_match.group(1)

    # Parse each feed block
    feed_blocks = tier1_section.split('---')

    for block in feed_blocks:
        block = block.strip()
        if not block:
            continue

        feed = {}

        # Extract fields using regex
        feed_id_match = re.search(r'Feed ID:\s*(\S+)', block)
        url_match = re.search(r'URL:\s*(https://[^\s]+)', block)
        church_match = re.search(r'Church:\s*(.+?)(?:\n|$)', block)
        duration_match = re.search(r'Avg Duration:\s*(\d+)\s*minutes', block)
        episodes_match = re.search(r'Episodes:\s*(\d+)', block)
        description_match = re.search(r'Description:\s*(.+?)(?:\n|$)', block)

        if feed_id_match and url_match:
            feed['feed_id'] = feed_id_match.group(1)
            feed['url'] = url_match.group(1)
            feed['church'] = church_match.group(1) if church_match else 'Unknown'
            feed['avg_duration'] = int(duration_match.group(1)) if duration_match else 30
            feed['episodes'] = int(episodes_match.group(1)) if episodes_match else 0
            feed['description'] = description_match.group(1) if description_match else ''

            # Check for exposition keywords
            feed['has_exposition'] = any(
                keyword in block.lower()
                for keyword in ['verse by verse', 'verse-by-verse', 'exposition', 'expository']
            )

            feeds.append(feed)

    return feeds


def rank_feeds(feeds: List[Dict]) -> List[Dict]:
    """Rank feeds by verse-by-verse exposition and 40+ min duration."""
    def score_feed(feed: Dict) -> Tuple[int, int, int]:
        # Priority 1: Has exposition keywords
        exposition_score = 1 if feed['has_exposition'] else 0

        # Priority 2: Duration >= 40 minutes
        duration_score = 1 if feed['avg_duration'] >= 40 else 0

        # Priority 3: More episodes (tiebreaker)
        episodes_score = feed['episodes']

        return (exposition_score, duration_score, episodes_score)

    return sorted(feeds, key=score_feed, reverse=True)


def download_audio(url: str, output_path: Path) -> bool:
    """Download audio file from URL."""
    try:
        logger.info(f"Downloading audio from {url}")
        response = requests.get(url, stream=True, timeout=300)
        response.raise_for_status()

        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        if output_path.exists() and output_path.stat().st_size > 0:
            logger.info(f"Downloaded {output_path.stat().st_size / (1024*1024):.1f} MB")
            return True
        return False
    except Exception as e:
        logger.error(f"Failed to download audio: {e}")
        return False


def transcribe_audio_whisper(audio_path: Path, episode_id: str) -> Optional[Dict]:
    """Transcribe audio using local Whisper model."""
    try:
        logger.info(f"Loading Whisper {WHISPER_MODEL} model...")
        model = whisper.load_model(WHISPER_MODEL)

        logger.info(f"Transcribing {audio_path.name}...")
        result = model.transcribe(str(audio_path), language='en', fp16=False)

        # Extract full text
        full_text = result.get('text', '').strip()
        if not full_text:
            logger.error(f"Empty transcription for {episode_id}")
            return None

        # Extract segments
        segments = []
        for seg in result.get('segments', []):
            segments.append({
                'start': seg['start'],
                'end': seg['end'],
                'text': seg['text'].strip()
            })

        word_count = len(full_text.split())
        language = result.get('language', 'en')

        logger.info(f"Transcribed {word_count} words ({language})")

        return {
            'full_text': full_text,
            'segments': segments,
            'language': language,
            'word_count': word_count
        }

    except Exception as e:
        logger.error(f"Whisper transcription failed for {episode_id}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


def process_feed(feed: Dict, max_episodes: int = 5) -> List[Dict]:
    """Process a single RSS feed and return episode info."""
    episodes_processed = []

    try:
        logger.info(f"\n{'='*80}")
        logger.info(f"Processing feed: {feed['church']} ({feed['feed_id']})")
        logger.info(f"URL: {feed['url']}")
        logger.info(f"{'='*80}")

        # Parse RSS feed
        parsed_feed = feedparser.parse(feed['url'])

        if not parsed_feed.entries:
            logger.warning(f"No entries found in feed {feed['feed_id']}")
            return episodes_processed

        # Get most recent episodes
        recent_episodes = parsed_feed.entries[:max_episodes]
        logger.info(f"Found {len(recent_episodes)} episodes to process")

        for idx, entry in enumerate(recent_episodes, 1):
            try:
                # Extract episode info
                title = entry.get('title', 'Untitled')
                published = entry.get('published_parsed') or entry.get('updated_parsed')
                published_str = time.strftime('%Y-%m-%d %H:%M:%S', published) if published else None

                # Get audio URL
                audio_url = None
                if hasattr(entry, 'enclosures') and entry.enclosures:
                    for enclosure in entry.enclosures:
                        if enclosure.get('type', '').startswith('audio/'):
                            audio_url = enclosure.get('href')
                            break

                if not audio_url and hasattr(entry, 'links'):
                    for link in entry.links:
                        if link.get('type', '').startswith('audio/'):
                            audio_url = link.get('href')
                            break

                if not audio_url:
                    logger.warning(f"No audio URL found for episode: {title}")
                    continue

                # Generate episode ID from URL
                episode_id = hashlib.md5(audio_url.encode()).hexdigest()[:16]

                # Get duration from iTunes tags
                duration_seconds = None
                itunes_duration = entry.get('itunes_duration')
                if itunes_duration:
                    duration_seconds = parse_duration_str(itunes_duration)

                logger.info(f"\n[{idx}/{len(recent_episodes)}] {title}")
                logger.info(f"Episode ID: {episode_id}")
                logger.info(f"Published: {published_str}")
                if duration_seconds:
                    logger.info(f"Duration: {duration_seconds // 60} minutes")

                # Create channel entry
                channel_id = feed['feed_id']
                db.upsert_channel(
                    channel_id=channel_id,
                    channel_name=feed['church'],
                    source_url=feed['url'],
                    resolved_via='rss_feed'
                )

                # Create video entry (using episode_id as video_id)
                db.insert_or_ignore_video(
                    video_id=episode_id,
                    channel_id=channel_id,
                    title=title,
                    published_at=published_str,
                    duration_seconds=duration_seconds or 0,
                    status='discovered'
                )

                # Check if already transcribed
                existing = db.get_transcript(episode_id)
                if existing:
                    logger.info(f"✓ Already transcribed (skipping)")
                    episodes_processed.append({
                        'episode_id': episode_id,
                        'title': title,
                        'status': 'already_transcribed'
                    })
                    continue

                # Download audio
                audio_path = TMP_AUDIO_DIR / f"{episode_id}.mp3"
                if not download_audio(audio_url, audio_path):
                    db.update_video_status(episode_id, 'error', 'Audio download failed')
                    continue

                # Transcribe
                db.update_video_status(episode_id, 'transcribing', None)
                transcript_data = transcribe_audio_whisper(audio_path, episode_id)

                if transcript_data:
                    # Save to database
                    success, error = db.insert_transcript(
                        video_id=episode_id,
                        full_text=transcript_data['full_text'],
                        segments_json=json.dumps(transcript_data['segments']),
                        language=transcript_data['language'],
                        word_count=transcript_data['word_count'],
                        transcript_provider='whisper_local',
                        transcript_model=WHISPER_MODEL,
                        transcript_version='v1.0'
                    )

                    if success:
                        db.update_video_status(episode_id, 'transcribed', None)
                        logger.info(f"✓ Successfully transcribed and saved to database")
                        episodes_processed.append({
                            'episode_id': episode_id,
                            'title': title,
                            'word_count': transcript_data['word_count'],
                            'status': 'success'
                        })
                    else:
                        logger.error(f"Failed to save transcript: {error}")
                        db.update_video_status(episode_id, 'error', error)
                else:
                    db.update_video_status(episode_id, 'error', 'Transcription failed')

                # Cleanup audio file
                try:
                    if audio_path.exists():
                        audio_path.unlink()
                        logger.info(f"Cleaned up audio file")
                except Exception:
                    pass

            except Exception as e:
                logger.error(f"Error processing episode: {e}")
                import traceback
                logger.error(traceback.format_exc())
                continue

    except Exception as e:
        logger.error(f"Error processing feed {feed['feed_id']}: {e}")
        import traceback
        logger.error(traceback.format_exc())

    return episodes_processed


def generate_report(selected_feeds: List[Dict], all_results: Dict, run_id: int):
    """Generate progress report."""
    report_file = Path(__file__).parent / f"rss_transcription_report_{run_id}.txt"

    total_episodes = sum(len(results) for results in all_results.values())
    successful = sum(
        1 for results in all_results.values()
        for r in results
        if r.get('status') == 'success'
    )
    already_done = sum(
        1 for results in all_results.values()
        for r in results
        if r.get('status') == 'already_transcribed'
    )
    failed = total_episodes - successful - already_done

    total_words = sum(
        r.get('word_count', 0) for results in all_results.values()
        for r in results
        if r.get('status') == 'success'
    )

    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("RSS FEED TRANSCRIPTION REPORT\n")
        f.write("=" * 80 + "\n\n")

        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Run ID: {run_id}\n")
        f.write(f"Whisper Model: {WHISPER_MODEL}\n\n")

        f.write("=" * 80 + "\n")
        f.write("SUMMARY\n")
        f.write("=" * 80 + "\n\n")

        f.write(f"Feeds Processed: {len(selected_feeds)}\n")
        f.write(f"Total Episodes: {total_episodes}\n")
        f.write(f"  - Successfully Transcribed: {successful}\n")
        f.write(f"  - Already Transcribed: {already_done}\n")
        f.write(f"  - Failed: {failed}\n")
        f.write(f"Total Words Transcribed: {total_words:,}\n\n")

        f.write("=" * 80 + "\n")
        f.write("SELECTED FEEDS (Top 15)\n")
        f.write("=" * 80 + "\n\n")

        for idx, feed in enumerate(selected_feeds, 1):
            f.write(f"{idx}. {feed['church']}\n")
            f.write(f"   Feed ID: {feed['feed_id']}\n")
            f.write(f"   Avg Duration: {feed['avg_duration']} min\n")
            f.write(f"   Total Episodes Available: {feed['episodes']}\n")
            f.write(f"   Verse-by-Verse: {'Yes' if feed['has_exposition'] else 'No'}\n")

            if feed['feed_id'] in all_results:
                results = all_results[feed['feed_id']]
                success_count = sum(1 for r in results if r.get('status') == 'success')
                f.write(f"   Episodes Processed: {len(results)}\n")
                f.write(f"   Successfully Transcribed: {success_count}\n")

            f.write("\n")

        f.write("=" * 80 + "\n")
        f.write("DETAILED RESULTS BY FEED\n")
        f.write("=" * 80 + "\n\n")

        for feed in selected_feeds:
            if feed['feed_id'] not in all_results:
                continue

            results = all_results[feed['feed_id']]
            if not results:
                continue

            f.write(f"### {feed['church']} ({feed['feed_id']})\n\n")

            for result in results:
                status_icon = {
                    'success': '✓',
                    'already_transcribed': '✓ (cached)',
                    'error': '✗'
                }.get(result.get('status'), '?')

                f.write(f"{status_icon} {result['title']}\n")
                if result.get('word_count'):
                    f.write(f"   Words: {result['word_count']:,}\n")
                f.write("\n")

            f.write("\n")

    logger.info(f"\n{'='*80}")
    logger.info(f"Report saved to: {report_file}")
    logger.info(f"{'='*80}")

    return report_file


def main():
    """Main entry point."""
    logger.info("=" * 80)
    logger.info("RSS FEED TRANSCRIPTION PIPELINE")
    logger.info("=" * 80)
    logger.info("")

    # Initialize database
    logger.info("Initializing database...")
    db.init_db()

    # Create run record
    run_id = db.create_run('rss_transcription')
    logger.info(f"Created run ID: {run_id}")

    try:
        # Step 1: Load Tier 1 feeds from both reports
        logger.info("\n" + "=" * 80)
        logger.info("STEP 1: Loading Tier 1 feeds from reports")
        logger.info("=" * 80)

        feeds1 = extract_tier1_feeds('sermon_feeds.txt')
        logger.info(f"Loaded {len(feeds1)} Tier 1 feeds from sermon_feeds.txt")

        feeds2 = extract_tier1_feeds('sermon_feeds_2.txt')
        logger.info(f"Loaded {len(feeds2)} Tier 1 feeds from sermon_feeds_2.txt")

        # Combine and deduplicate
        all_feeds = {}
        for feed in feeds1 + feeds2:
            if feed['feed_id'] not in all_feeds:
                all_feeds[feed['feed_id']] = feed

        unique_feeds = list(all_feeds.values())
        logger.info(f"Total unique Tier 1 feeds: {len(unique_feeds)}")

        # Step 2: Rank and select top 15
        logger.info("\n" + "=" * 80)
        logger.info("STEP 2: Ranking feeds by criteria")
        logger.info("=" * 80)

        ranked_feeds = rank_feeds(unique_feeds)
        selected_feeds = ranked_feeds[:15]

        logger.info(f"Selected top {len(selected_feeds)} feeds:")
        for idx, feed in enumerate(selected_feeds, 1):
            exposition = "✓" if feed['has_exposition'] else "✗"
            duration = "✓" if feed['avg_duration'] >= 40 else "✗"
            logger.info(
                f"  {idx}. {feed['church']} - "
                f"Exposition:{exposition} Duration:{duration} ({feed['avg_duration']}min)"
            )

        # Step 3-5: Process each feed
        logger.info("\n" + "=" * 80)
        logger.info("STEP 3-5: Downloading and transcribing episodes")
        logger.info("=" * 80)

        all_results = {}
        total_processed = 0
        total_minutes = 0.0

        for idx, feed in enumerate(selected_feeds, 1):
            logger.info(f"\n### Feed {idx}/{len(selected_feeds)}")
            results = process_feed(feed, max_episodes=5)
            all_results[feed['feed_id']] = results
            total_processed += len([r for r in results if r.get('status') == 'success'])

            # Estimate minutes (rough: 1 sermon ≈ 40 min avg)
            total_minutes += len(results) * 40

        # Step 6: Generate report
        logger.info("\n" + "=" * 80)
        logger.info("STEP 6: Generating progress report")
        logger.info("=" * 80)

        report_file = generate_report(selected_feeds, all_results, run_id)

        # Finish run
        db.finish_run(
            run_id=run_id,
            status='completed',
            videos_processed=total_processed,
            minutes_processed=total_minutes,
            notes=f'RSS transcription complete. Report: {report_file.name}'
        )

        # Print final stats
        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE COMPLETE!")
        logger.info("=" * 80)
        logger.info(f"Total sermons transcribed: {total_processed}")
        logger.info(f"Estimated audio processed: {total_minutes:.0f} minutes")
        logger.info(f"Report: {report_file}")
        logger.info("=" * 80)

        # Print database stats
        stats = db.get_db_stats()
        logger.info("\nDatabase Statistics:")
        logger.info(f"  Channels: {stats['channels']}")
        logger.info(f"  Videos: {stats['videos']}")
        logger.info(f"  Transcripts: {stats['transcripts']}")

    except Exception as e:
        logger.error(f"\nPipeline failed: {e}")
        import traceback
        logger.error(traceback.format_exc())

        db.finish_run(
            run_id=run_id,
            status='failed',
            videos_processed=0,
            minutes_processed=0,
            notes=f'Error: {str(e)}'
        )
        sys.exit(1)


if __name__ == '__main__':
    main()
