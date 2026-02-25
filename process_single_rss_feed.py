#!/usr/bin/env python3
"""
Process a single RSS feed URL and transcribe episodes.
"""

import os
import sys
import json
import logging
import hashlib
import re
import time
from pathlib import Path
from typing import List, Dict, Optional

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
WHISPER_MODEL = "base"


def parse_duration_str(duration_str: str) -> Optional[int]:
    """Parse iTunes duration string to seconds."""
    if not duration_str or not str(duration_str).strip():
        return None

    duration_str = str(duration_str).strip()

    try:
        return int(duration_str)
    except ValueError:
        pass

    parts = duration_str.split(':')
    try:
        if len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        elif len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    except (ValueError, IndexError):
        pass

    return None


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

        full_text = result.get('text', '').strip()
        if not full_text:
            logger.error(f"Empty transcription for {episode_id}")
            return None

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


def process_feed(rss_url: str, max_episodes: int = 5) -> List[Dict]:
    """Process a single RSS feed and return episode info."""
    episodes_processed = []

    try:
        logger.info(f"{'='*80}")
        logger.info(f"Processing RSS feed: {rss_url}")
        logger.info(f"{'='*80}")

        parsed_feed = feedparser.parse(rss_url)

        if not parsed_feed.entries:
            logger.warning(f"No entries found in feed")
            return episodes_processed

        # Get feed info
        feed_title = parsed_feed.feed.get('title', 'Unknown Podcast')
        logger.info(f"Feed Title: {feed_title}")

        recent_episodes = parsed_feed.entries[:max_episodes]
        logger.info(f"Found {len(recent_episodes)} episodes to process")

        # Generate channel ID from RSS URL
        channel_id = hashlib.md5(rss_url.encode()).hexdigest()[:16]

        for idx, entry in enumerate(recent_episodes, 1):
            try:
                title = entry.get('title', 'Untitled')
                published = entry.get('published_parsed') or entry.get('updated_parsed')
                published_str = time.strftime('%Y-%m-%d %H:%M:%S', published) if published else None

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

                episode_id = hashlib.md5(audio_url.encode()).hexdigest()[:16]

                duration_seconds = None
                itunes_duration = entry.get('itunes_duration')
                if itunes_duration:
                    duration_seconds = parse_duration_str(itunes_duration)

                logger.info(f"\n[{idx}/{len(recent_episodes)}] {title}")
                logger.info(f"Episode ID: {episode_id}")
                logger.info(f"Published: {published_str}")
                if duration_seconds:
                    logger.info(f"Duration: {duration_seconds // 60} minutes")

                db.upsert_channel(
                    channel_id=channel_id,
                    channel_name=feed_title,
                    source_url=rss_url,
                    resolved_via='rss_feed'
                )

                db.insert_or_ignore_video(
                    video_id=episode_id,
                    channel_id=channel_id,
                    title=title,
                    published_at=published_str,
                    duration_seconds=duration_seconds or 0,
                    status='discovered'
                )

                existing = db.get_transcript(episode_id)
                if existing:
                    logger.info(f"✓ Already transcribed (skipping)")
                    episodes_processed.append({
                        'episode_id': episode_id,
                        'title': title,
                        'status': 'already_transcribed'
                    })
                    continue

                audio_path = TMP_AUDIO_DIR / f"{episode_id}.mp3"
                if not download_audio(audio_url, audio_path):
                    db.update_video_status(episode_id, 'error', 'Audio download failed')
                    continue

                db.update_video_status(episode_id, 'transcribing', None)
                transcript_data = transcribe_audio_whisper(audio_path, episode_id)

                if transcript_data:
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
        logger.error(f"Error processing feed: {e}")
        import traceback
        logger.error(traceback.format_exc())

    return episodes_processed


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python process_single_rss_feed.py <rss_url>")
        sys.exit(1)

    rss_url = sys.argv[1]

    logger.info("=" * 80)
    logger.info("SINGLE RSS FEED TRANSCRIPTION")
    logger.info("=" * 80)
    logger.info(f"RSS URL: {rss_url}\n")

    # Initialize database
    db.init_db()
    run_id = db.create_run('single_rss_transcription')
    logger.info(f"Created run ID: {run_id}\n")

    try:
        results = process_feed(rss_url, max_episodes=5)

        total_processed = len([r for r in results if r.get('status') == 'success'])
        total_words = sum(r.get('word_count', 0) for r in results if r.get('status') == 'success')

        # Finish run
        db.finish_run(
            run_id=run_id,
            status='completed',
            videos_processed=total_processed,
            minutes_processed=total_processed * 40.0,  # Estimate
            notes=f'Single RSS feed transcription complete'
        )

        logger.info("\n" + "=" * 80)
        logger.info("TRANSCRIPTION COMPLETE!")
        logger.info("=" * 80)
        logger.info(f"Episodes transcribed: {total_processed}")
        logger.info(f"Total words: {total_words:,}")
        logger.info("=" * 80)

        stats = db.get_db_stats()
        logger.info("\nDatabase Statistics:")
        logger.info(f"  Channels: {stats['channels']}")
        logger.info(f"  Videos: {stats['videos']}")
        logger.info(f"  Transcripts: {stats['transcripts']}")

    except Exception as e:
        logger.error(f"\nTranscription failed: {e}")
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
