import os
import json
import subprocess
import logging
import traceback
from pathlib import Path

from openai import OpenAI
from openai import APIStatusError

from engine.config import OPENAI_API_KEY, TMP_AUDIO_DIR, KEEP_AUDIO_ON_FAIL
from engine import db

logger = logging.getLogger("digital_pulpit")

TRANSCRIPTION_MODEL = "whisper-1"

# OpenAI server error shows: Maximum content size limit (26214400) exceeded
MAX_UPLOAD_BYTES = 26_214_400

# If we must chunk, chunk into 10-minute segments
CHUNK_SECONDS = 10 * 60


def _file_size(path: str) -> int:
    try:
        return Path(path).stat().st_size
    except Exception:
        return 0


def download_audio(video_id: str) -> str | None:
    """
    Downloads best audio as MP3 using yt-dlp.
    Returns path to mp3, or None on failure.
    """
    os.makedirs(TMP_AUDIO_DIR, exist_ok=True)
    output_path = os.path.join(TMP_AUDIO_DIR, f"{video_id}.mp3")

    if os.path.exists(output_path):
        logger.info(f"Audio already exists: {output_path}")
        return output_path

    url = f"https://www.youtube.com/watch?v={video_id}"

    # NOTE: --audio-quality uses ffmpeg "quality scale" for VBR in some modes;
    # this can still produce large files for long videos.
    cookies_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "cookies.txt")
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
    ]
    if os.path.exists(cookies_path) and os.path.getsize(cookies_path) > 0:
        cmd.extend(["--cookies", cookies_path])
    cmd.append(url)

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode != 0:
            logger.error(f"yt-dlp failed for {video_id}: {result.stderr}")
            return None

        if os.path.exists(output_path):
            logger.info(f"Downloaded audio: {output_path}")
            return output_path

        # Sometimes yt-dlp/ffmpeg produces odd suffixes; try to recover.
        alt_path = output_path.replace(".mp3", ".mp3.mp3")
        if os.path.exists(alt_path):
            os.rename(alt_path, output_path)
            logger.info(f"Downloaded audio (renamed): {output_path}")
            return output_path

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
        logger.error(traceback.format_exc())
        return None


def _reencode_mp3(input_path: str, output_path: str, bitrate_kbps: int) -> bool:
    """
    Re-encode audio to mono 16kHz MP3 at specified bitrate.
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
        f"{bitrate_kbps}k",
        output_path,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"ffmpeg re-encode failed ({bitrate_kbps}k): {result.stderr}")
        return False
    return os.path.exists(output_path)


def _split_into_chunks(input_path: str, out_dir: str, chunk_seconds: int, bitrate_kbps: int = 32) -> list[str]:
    """
    Split audio into N-second chunks and re-encode each chunk to mono 16kHz MP3.
    Using re-encode during segmentation keeps chunk files small & consistent.
    """
    os.makedirs(out_dir, exist_ok=True)
    # ffmpeg segment muxer output pattern
    out_pattern = os.path.join(out_dir, "chunk_%03d.mp3")

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
        f"{bitrate_kbps}k",
        "-f",
        "segment",
        "-segment_time",
        str(chunk_seconds),
        "-reset_timestamps",
        "1",
        out_pattern,
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"ffmpeg chunking failed: {result.stderr}")
        return []

    chunks = sorted(
        str(Path(out_dir) / p)
        for p in os.listdir(out_dir)
        if p.startswith("chunk_") and p.endswith(".mp3")
    )
    return chunks


def _looks_like_413(exc: Exception) -> bool:
    """
    Detect OpenAI 413 errors in a robust way.
    """
    if isinstance(exc, APIStatusError):
        # APIStatusError has .status_code
        try:
            return getattr(exc, "status_code", None) == 413
        except Exception:
            pass

    msg = str(exc) or ""
    return "413" in msg and "Maximum content size limit" in msg


def _response_to_text_segments(response, offset_seconds: float = 0.0) -> tuple[str, list[dict], str]:
    """
    Convert OpenAI verbose_json response to (text, segments, language).
    Adds offset to segment start/end.
    """
    full_text = getattr(response, "text", "") or ""
    language = getattr(response, "language", "en") or "en"

    segments: list[dict] = []
    if hasattr(response, "segments") and response.segments:
        for seg in response.segments:
            start = float(getattr(seg, "start", 0.0) or 0.0) + float(offset_seconds)
            end = float(getattr(seg, "end", 0.0) or 0.0) + float(offset_seconds)
            text = getattr(seg, "text", "") or ""
            segments.append({"start": start, "end": end, "text": text})

    return full_text, segments, language


def transcribe_audio(video_id: str, audio_path: str) -> tuple[bool, str | None]:
    """
    Returns: (success: bool, error_message: str|None)

    Strategy:
    - Attempt original file if <= MAX_UPLOAD_BYTES.
    - If too large or 413: re-encode at 48k then 32k.
    - If still too large: chunk into 10-minute segments at 32k and stitch.
    """
    if not OPENAI_API_KEY:
        return False, "OPENAI_API_KEY not set"

    client = OpenAI(api_key=OPENAI_API_KEY)

    def _attempt(path_to_use: str, label: str):
        with open(path_to_use, "rb") as audio_file:
            logger.info(f"Transcribing {video_id} ({label})... size={_file_size(path_to_use)} bytes")
            return client.audio.transcriptions.create(
                model=TRANSCRIPTION_MODEL,
                file=audio_file,
                response_format="verbose_json",
                timestamp_granularities=["segment"],
            )

    # If original is too big, don’t even try (avoid guaranteed 413).
    original_size = _file_size(audio_path)
    if original_size <= 0:
        return False, f"Audio file missing or unreadable: {audio_path}"

    work_paths_to_cleanup: list[str] = []

    # Helper to try transcription with automatic downsize steps
    def _try_with_downsize() -> tuple[object | None, str | None, str | None]:
        """
        Returns: (response_or_none, error_or_none, used_path_or_none)
        """
        # 1) Try original if it's under the limit
        if original_size <= MAX_UPLOAD_BYTES:
            try:
                resp = _attempt(audio_path, "original mp3")
                return resp, None, audio_path
            except Exception as e:
                err = f"{type(e).__name__}: {str(e)}"
                logger.error(f"Transcription failed for {video_id} (original): {err}")
                logger.error(traceback.format_exc())

                # If not 413, we still try re-encode once (sometimes fixes weird MP3 headers)
                # but keep the error context.
                original_err = err
        else:
            original_err = f"File too large: {original_size} bytes"

        # 2) Re-encode at 48k
        fixed_48 = audio_path.replace(".mp3", "_fixed_48k.mp3")
        try:
            ok = _reencode_mp3(audio_path, fixed_48, bitrate_kbps=48)
            if not ok:
                return None, f"ffmpeg re-encode failed (48k); original error: {original_err}", None
            work_paths_to_cleanup.append(fixed_48)

            size_48 = _file_size(fixed_48)
            if size_48 > MAX_UPLOAD_BYTES:
                logger.warning(
                    f"Re-encoded 48k still too big ({size_48} bytes) for {video_id}, trying 32k..."
                )
            else:
                try:
                    resp = _attempt(fixed_48, "re-encoded mp3 48k")
                    return resp, None, fixed_48
                except Exception as e:
                    err = f"{type(e).__name__}: {str(e)}"
                    logger.error(f"Transcription failed for {video_id} (48k): {err}")
                    logger.error(traceback.format_exc())

                    if not _looks_like_413(e):
                        return None, err, None
        except Exception as e:
            err = f"{type(e).__name__}: {str(e)}"
            logger.error(f"Re-encode attempt (48k) crashed for {video_id}: {err}")
            logger.error(traceback.format_exc())

        # 3) Re-encode at 32k
        fixed_32 = audio_path.replace(".mp3", "_fixed_32k.mp3")
        try:
            ok = _reencode_mp3(audio_path, fixed_32, bitrate_kbps=32)
            if not ok:
                return None, f"ffmpeg re-encode failed (32k); original error: {original_err}", None
            work_paths_to_cleanup.append(fixed_32)

            size_32 = _file_size(fixed_32)
            if size_32 <= MAX_UPLOAD_BYTES:
                try:
                    resp = _attempt(fixed_32, "re-encoded mp3 32k")
                    return resp, None, fixed_32
                except Exception as e:
                    err = f"{type(e).__name__}: {str(e)}"
                    logger.error(f"Transcription failed for {video_id} (32k): {err}")
                    logger.error(traceback.format_exc())
                    return None, err, None
            else:
                logger.warning(
                    f"Re-encoded 32k still too big ({size_32} bytes) for {video_id}, will chunk..."
                )
        except Exception as e:
            err = f"{type(e).__name__}: {str(e)}"
            logger.error(f"Re-encode attempt (32k) crashed for {video_id}: {err}")
            logger.error(traceback.format_exc())

        return None, "Audio still too large after re-encode; chunking required", None

    try:
        # First try direct / re-encode path
        response, err_msg, used_path = _try_with_downsize()

        if response is not None:
            # Single-shot transcript
            full_text, segments, language = _response_to_text_segments(response, offset_seconds=0.0)
            word_count = len(full_text.split())

            db.insert_transcript(
                video_id,
                full_text,
                json.dumps(segments),
                language,
                word_count,
                TRANSCRIPTION_MODEL,
            )
            db.update_video_status(video_id, "transcribed", None)
            logger.info(f"Transcribed {video_id}: {word_count} words, language={language}")
            return True, None

        # If we get here, we need chunking.
        logger.info(f"Chunking audio for {video_id} because: {err_msg}")

        # Prefer chunking from the smallest available re-encode if it exists
        chunk_source = None
        for candidate in [audio_path.replace(".mp3", "_fixed_32k.mp3"), audio_path.replace(".mp3", "_fixed_48k.mp3"), audio_path]:
            if os.path.exists(candidate):
                chunk_source = candidate
                break
        if not chunk_source:
            return False, f"Chunk source missing for {video_id}"

        chunks_dir = os.path.join(TMP_AUDIO_DIR, f"{video_id}_chunks")
        chunks = _split_into_chunks(chunk_source, chunks_dir, CHUNK_SECONDS, bitrate_kbps=32)

        if not chunks:
            db.update_video_status(video_id, "error", "Chunking failed (ffmpeg segment)")
            return False, "Chunking failed (ffmpeg segment)"

        stitched_text_parts: list[str] = []
        stitched_segments: list[dict] = []
        language_final = "en"

        # Transcribe each chunk and offset timestamps by chunk start
        for idx, chunk_path in enumerate(chunks):
            offset = float(idx * CHUNK_SECONDS)

            # Ensure chunk is under limit; if not, re-encode chunk harder (rare)
            if _file_size(chunk_path) > MAX_UPLOAD_BYTES:
                smaller = chunk_path.replace(".mp3", "_smaller.mp3")
                ok = _reencode_mp3(chunk_path, smaller, bitrate_kbps=24)
                if ok:
                    work_paths_to_cleanup.append(smaller)
                    chunk_path = smaller

            try:
                resp = _attempt(chunk_path, f"chunk {idx+1}/{len(chunks)}")
            except Exception as e:
                err = f"{type(e).__name__}: {str(e)}"
                logger.error(f"Chunk transcription failed for {video_id} chunk {idx}: {err}")
                logger.error(traceback.format_exc())
                db.update_video_status(video_id, "error", f"Chunk {idx} failed: {err}")
                return False, f"Chunk {idx} failed: {err}"

            text, segs, lang = _response_to_text_segments(resp, offset_seconds=offset)
            if lang:
                language_final = lang

            if text.strip():
                stitched_text_parts.append(text.strip())
            if segs:
                stitched_segments.extend(segs)

        full_text = "\n\n".join(stitched_text_parts).strip()
        word_count = len(full_text.split())

        db.insert_transcript(
            video_id,
            full_text,
            json.dumps(stitched_segments),
            language_final,
            word_count,
            TRANSCRIPTION_MODEL,
        )
        db.update_video_status(video_id, "transcribed", None)
        logger.info(f"Transcribed {video_id} via chunks: {word_count} words, language={language_final}")
        return True, None

    except Exception as e:
        err = f"{type(e).__name__}: {str(e)}"
        logger.error(f"Post-processing failed for {video_id}: {err}")
        logger.error(traceback.format_exc())
        db.update_video_status(video_id, "error", err)
        return False, err

    finally:
        # Clean up any re-encoded temp files we created (not the original mp3 unless cleanup_audio handles it)
        for p in work_paths_to_cleanup:
            try:
                if os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass


def cleanup_audio(video_id: str, success: bool):
    """
    Removes audio file after transcription depending on KEEP_AUDIO_ON_FAIL.
    """
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
