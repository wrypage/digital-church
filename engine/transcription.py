import os
import json
import subprocess
import logging
from openai import OpenAI
from engine.config import OPENAI_API_KEY, TMP_AUDIO_DIR, KEEP_AUDIO_ON_FAIL
from engine import db

logger = logging.getLogger("digital_pulpit")

TRANSCRIPTION_MODEL = "whisper-1"


def download_audio(video_id):
    os.makedirs(TMP_AUDIO_DIR, exist_ok=True)
    output_path = os.path.join(TMP_AUDIO_DIR, f"{video_id}.mp3")

    if os.path.exists(output_path):
        logger.info(f"Audio already exists: {output_path}")
        return output_path

    url = f"https://www.youtube.com/watch?v={video_id}"
    cmd = [
        "yt-dlp",
        "-f", "bestaudio",
        "--extract-audio",
        "--audio-format", "mp3",
        "--audio-quality", "5",
        "-o", output_path,
        "--no-playlist",
        "--no-warnings",
        url,
    ]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        if result.returncode != 0:
            logger.error(f"yt-dlp failed for {video_id}: {result.stderr}")
            return None
        if os.path.exists(output_path):
            logger.info(f"Downloaded audio: {output_path}")
            return output_path
        alt_path = output_path.replace(".mp3", ".mp3.mp3")
        if os.path.exists(alt_path):
            os.rename(alt_path, output_path)
            return output_path
        for f in os.listdir(TMP_AUDIO_DIR):
            if f.startswith(video_id):
                found = os.path.join(TMP_AUDIO_DIR, f)
                os.rename(found, output_path)
                return output_path
        logger.error(f"Audio file not found after download for {video_id}")
        return None
    except subprocess.TimeoutExpired:
        logger.error(f"yt-dlp timed out for {video_id}")
        return None
    except Exception as e:
        logger.error(f"Download error for {video_id}: {e}")
        return None


def transcribe_audio(video_id, audio_path):
    if not OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY not set")

    client = OpenAI(api_key=OPENAI_API_KEY)

    try:
        with open(audio_path, "rb") as audio_file:
            response = client.audio.transcriptions.create(
                model=TRANSCRIPTION_MODEL,
                file=audio_file,
                response_format="verbose_json",
                timestamp_granularities=["segment"],
            )

        full_text = response.text
        segments = []
        if hasattr(response, "segments") and response.segments:
            for seg in response.segments:
                seg_start = getattr(seg, "start", 0)
                seg_end = getattr(seg, "end", 0)
                seg_text = getattr(seg, "text", "")
                segments.append({
                    "start": seg_start,
                    "end": seg_end,
                    "text": seg_text,
                })

        language = getattr(response, "language", "en")
        word_count = len(full_text.split())

        db.insert_transcript(
            video_id, full_text, json.dumps(segments),
            language, word_count, TRANSCRIPTION_MODEL
        )
        db.update_video_status(video_id, "transcribed")

        logger.info(f"Transcribed {video_id}: {word_count} words, language={language}")
        return True

    except Exception as e:
        logger.error(f"Transcription failed for {video_id}: {e}")
        db.update_video_status(video_id, "failed", str(e))
        return False


def cleanup_audio(video_id, success):
    audio_path = os.path.join(TMP_AUDIO_DIR, f"{video_id}.mp3")
    if os.path.exists(audio_path):
        if success or not KEEP_AUDIO_ON_FAIL:
            os.remove(audio_path)
            logger.info(f"Cleaned up audio: {audio_path}")
        else:
            logger.info(f"Keeping audio on fail: {audio_path}")
