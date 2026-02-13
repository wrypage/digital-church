import os
import json
import subprocess
import logging
import traceback
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
        "-f",
        "bestaudio",
        "--extract-audio",
        "--audio-format",
        "mp3",
        "--audio-quality",
        "5",
        "-o",
        output_path,
        "--no-playlist",
        "--no-warnings",
        url,
    ]

    try:
        result = subprocess.run(cmd,
                                capture_output=True,
                                text=True,
                                timeout=600)
        if result.returncode != 0:
            logger.error(f"yt-dlp failed for {video_id}: {result.stderr}")
            return None

        if os.path.exists(output_path):
            logger.info(f"Downloaded audio: {output_path}")
            return output_path

        # Replit/yt-dlp sometimes adds weird double extensions
        alt_path = output_path.replace(".mp3", ".mp3.mp3")
        if os.path.exists(alt_path):
            os.rename(alt_path, output_path)
            logger.info(f"Downloaded audio (renamed): {output_path}")
            return output_path

        # Last resort: find any file that starts with video_id
        for f in os.listdir(TMP_AUDIO_DIR):
            if f.startswith(video_id):
                found = os.path.join(TMP_AUDIO_DIR, f)
                os.rename(found, output_path)
                logger.info(f"Downloaded audio (found/renamed): {output_path}")
                return output_path

        logger.error(f"Audio file not found after download for {video_id}")
        return None

    except subprocess.TimeoutExpired:
        logger.error(f"yt-dlp timed out for {video_id}")
        return None
    except Exception as e:
        logger.error(f"Download error for {video_id}: {e}")
        logger.debug(traceback.format_exc())
        return None


def _reencode_mp3(input_path, output_path):
    """
    Re-encode to a 'boring' mp3 that Whisper tends to accept:
    - 16kHz mono
    """
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        input_path,
        "-ac",
        "1",
        "-ar",
        "16000",
        "-vn",
        "-codec:a",
        "libmp3lame",
        "-b:a",
        "64k",
        output_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"ffmpeg re-encode failed: {result.stderr}")
        return False
    return os.path.exists(output_path)


def transcribe_audio(video_id, audio_path):
    if not OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY not set")

    client = OpenAI(api_key=OPENAI_API_KEY)

    def _attempt(path_to_use, attempt_label):
        with open(path_to_use, "rb") as audio_file:
            logger.info(f"Transcribing {video_id} ({attempt_label})...")
            return client.audio.transcriptions.create(
                model=TRANSCRIPTION_MODEL,
                file=audio_file,
                response_format="verbose_json",
                timestamp_granularities=["segment"],
            )

    try:
        # Attempt 1: raw mp3
        response = _attempt(audio_path, "original mp3")

    except Exception as e1:
        # Attempt 2: re-encode with ffmpeg and try again
        logger.error(
            f"Transcription attempt failed for {video_id} (original mp3): {e1}"
        )
        logger.error(traceback.format_exc())

        fixed_path = audio_path.replace(".mp3", "_fixed.mp3")
        try:
            ok = _reencode_mp3(audio_path, fixed_path)
            if not ok:
                raise RuntimeError(
                    "Re-encode failed; cannot retry transcription")

            response = _attempt(fixed_path, "re-encoded mp3")

        except Exception as e2:
            logger.error(
                f"Transcription failed for {video_id} (after re-encode): {e2}")
            logger.error(traceback.format_exc())
            db.update_video_status(video_id, "failed",
                                   f"transcription_error: {str(e2)}")
            return False
        finally:
            # Clean up re-encoded file if we created it
            if os.path.exists(fixed_path):
                try:
                    os.remove(fixed_path)
                except Exception:
                    pass

    try:
        full_text = getattr(response, "text", "") or ""
        segments = []
        if hasattr(response, "segments") and response.segments:
            for seg in response.segments:
                seg_start = getattr(seg, "start", 0)
                seg_end = getattr(seg, "end", 0)
                seg_text = getattr(seg, "text", "")
                segments.append({
                    "start": seg_start,
                    "end": seg_end,
                    "text": seg_text
                })

        language = getattr(response, "language", "en")
        word_count = len(full_text.split())

        db.insert_transcript(video_id, full_text, json.dumps(segments),
                             language, word_count, TRANSCRIPTION_MODEL)
        db.update_video_status(video_id, "transcribed")

        logger.info(
            f"Transcribed {video_id}: {word_count} words, language={language}")
        return True

    except Exception as e3:
        logger.error(f"Post-processing failed for {video_id}: {e3}")
        logger.error(traceback.format_exc())
        db.update_video_status(video_id, "failed",
                               f"postprocess_error: {str(e3)}")
        return False


def cleanup_audio(video_id, success):
    audio_path = os.path.join(TMP_AUDIO_DIR, f"{video_id}.mp3")
    if os.path.exists(audio_path):
        if success or not KEEP_AUDIO_ON_FAIL:
            try:
                os.remove(audio_path)
                logger.info(f"Cleaned up audio: {audio_path}")
            except Exception:
                pass
        else:
            logger.info(f"Keeping audio on fail: {audio_path}")
