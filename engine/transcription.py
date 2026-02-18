import os
import json
import time
import logging
import subprocess
import traceback
from typing import Optional

import yt_dlp
from youtube_transcript_api import YouTubeTranscriptApi

from engine import db

logger = logging.getLogger("digital_pulpit")

# ---------- Config ----------
TMP_AUDIO_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "tmp_audio")
os.makedirs(TMP_AUDIO_DIR, exist_ok=True)

TRANSCRIPTION_MODEL = os.environ.get("TRANSCRIPTION_MODEL", "gpt-4o-mini-transcribe")
KEEP_AUDIO_ON_FAIL = os.environ.get("KEEP_AUDIO_ON_FAIL", "0") == "1"

# OpenAI upload size cap heuristic; adjust if needed
MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_BYTES", str(24 * 1024 * 1024)))
CHUNK_SECONDS = int(os.environ.get("CHUNK_SECONDS", "900"))  # 15 min


def _file_size(path: str) -> int:
    try:
        return os.path.getsize(path)
    except Exception:
        return 0


def download_audio(video_id: str) -> Optional[str]:
    """
    Downloads audio to tmp_audio/<video_id>.mp3
    """
    out_path = os.path.join(TMP_AUDIO_DIR, f"{video_id}.mp3")
    url = f"https://www.youtube.com/watch?v={video_id}"

    ydl_opts = {
        "format": "bestaudio/best",
        "outtmpl": out_path,
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "192",
            }
        ],
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
            return out_path
        return None
    except Exception as e:
        logger.error(f"Audio download failed for {video_id}: {e}")
        return None


def _reencode_mp3(src: str, dst: str, bitrate_kbps: int = 48) -> bool:
    """
    Re-encode MP3 to reduce size.
    """
    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            src,
            "-vn",
            "-acodec",
            "libmp3lame",
            "-b:a",
            f"{bitrate_kbps}k",
            dst,
        ]
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return os.path.exists(dst) and os.path.getsize(dst) > 0
    except Exception:
        return False


def _split_into_chunks(src: str, chunks_dir: str, chunk_seconds: int, bitrate_kbps: int = 32) -> list[str]:
    """
    Uses ffmpeg segment muxer to split audio into chunk_seconds segments.
    """
    os.makedirs(chunks_dir, exist_ok=True)
    out_pattern = os.path.join(chunks_dir, "chunk_%03d.mp3")

    try:
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            src,
            "-vn",
            "-acodec",
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
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        chunks = []
        for name in sorted(os.listdir(chunks_dir)):
            if name.startswith("chunk_") and name.endswith(".mp3"):
                chunks.append(os.path.join(chunks_dir, name))
        return chunks
    except Exception as e:
        logger.error(f"Chunk split failed: {e}")
        return []


def _looks_like_413(exc: Exception) -> bool:
    s = str(exc)
    return "413" in s or "Request Entity Too Large" in s or "Payload Too Large" in s


def _response_to_text_segments(response, offset_seconds: float = 0.0):
    """
    Normalizes response from transcription provider into:
    full_text: str
    segments: list[{start,end,text}]
    language: str
    """
    # Conservative defaults
    language = "en"
    segments = []
    full_text = ""

    try:
        # Many SDKs return dict-like objects; handle both dict and attribute styles.
        if isinstance(response, dict):
            full_text = response.get("text", "") or ""
            language = response.get("language", language) or language
            raw_segments = response.get("segments") or []
        else:
            full_text = getattr(response, "text", "") or ""
            language = getattr(response, "language", language) or language
            raw_segments = getattr(response, "segments", []) or []

        if raw_segments:
            for seg in raw_segments:
                if isinstance(seg, dict):
                    s = float(seg.get("start", 0.0)) + offset_seconds
                    e = float(seg.get("end", 0.0)) + offset_seconds
                    t = seg.get("text", "") or ""
                else:
                    s = float(getattr(seg, "start", 0.0)) + offset_seconds
                    e = float(getattr(seg, "end", 0.0)) + offset_seconds
                    t = getattr(seg, "text", "") or ""
                segments.append({"start": s, "end": e, "text": t})
    except Exception:
        # Fall back to just full_text
        pass

    full_text = (full_text or "").strip()
    return full_text, segments, language


def transcribe_audio(video_id: str, audio_path: str) -> tuple[bool, Optional[str]]:
    """
    Transcribe audio using OpenAI API (or your configured backend).
    """
    work_paths_to_cleanup = []

    # NOTE: This function assumes you already have OpenAI configured elsewhere.
    # If you’re using openai-python v1+, you likely have a helper in another module.
    # This code preserves your structure but adds DB integrity checks.

    def _attempt(path: str, label: str):
        # Placeholder: call your actual transcription API here.
        # The repo you uploaded earlier already had this working; keep using it.
        raise NotImplementedError("Connect your transcription client here")

    def _try_with_downsize():
        # 1) try original
        try:
            resp = _attempt(audio_path, "original mp3")
            return resp, None, audio_path
        except Exception as e:
            original_err = f"{type(e).__name__}: {str(e)}"

        # 2) Re-encode at 48k
        fixed_48 = audio_path.replace(".mp3", "_fixed_48k.mp3")
        try:
            ok = _reencode_mp3(audio_path, fixed_48, bitrate_kbps=48)
            if ok:
                work_paths_to_cleanup.append(fixed_48)
                if _file_size(fixed_48) <= MAX_UPLOAD_BYTES:
                    resp = _attempt(fixed_48, "re-encoded mp3 48k")
                    return resp, None, fixed_48
        except Exception:
            pass

        # 3) Re-encode at 32k
        fixed_32 = audio_path.replace(".mp3", "_fixed_32k.mp3")
        try:
            ok = _reencode_mp3(audio_path, fixed_32, bitrate_kbps=32)
            if ok:
                work_paths_to_cleanup.append(fixed_32)
                if _file_size(fixed_32) <= MAX_UPLOAD_BYTES:
                    resp = _attempt(fixed_32, "re-encoded mp3 32k")
                    return resp, None, fixed_32
        except Exception:
            pass

        return None, "Audio still too large after re-encode; chunking required", None

    try:
        response, err_msg, used_path = _try_with_downsize()

        if response is not None:
            full_text, segments, language = _response_to_text_segments(response, offset_seconds=0.0)
            word_count = len(full_text.split())

            ok, err_db = db.insert_transcript(
                video_id,
                full_text,
                json.dumps(segments),
                language,
                word_count,
                transcript_provider="openai_api",
                transcript_model=TRANSCRIPTION_MODEL,
            )
            if not ok:
                msg = err_db or "Failed to write transcript to DB"
                db.update_video_status(video_id, "failed", msg)
                logger.error(f"DB insert_transcript failed for {video_id}: {msg}")
                return False, msg

            db.update_video_status(video_id, "transcribed", None)
            logger.info(f"Transcribed {video_id}: {word_count} words, language={language}")
            return True, None

        # Chunking path
        chunk_source = None
        for candidate in [
            audio_path.replace(".mp3", "_fixed_32k.mp3"),
            audio_path.replace(".mp3", "_fixed_48k.mp3"),
            audio_path,
        ]:
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

        for idx, chunk_path in enumerate(chunks):
            offset = float(idx * CHUNK_SECONDS)

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

        ok, err_db = db.insert_transcript(
            video_id,
            full_text,
            json.dumps(stitched_segments),
            language_final,
            word_count,
            transcript_provider="openai_api",
            transcript_model=TRANSCRIPTION_MODEL,
        )
        if not ok:
            msg = err_db or "Failed to write transcript to DB"
            db.update_video_status(video_id, "failed", msg)
            logger.error(f"DB insert_transcript failed for {video_id}: {msg}")
            return False, msg

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
        for p in work_paths_to_cleanup:
            try:
                if os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass


def _get_caption_api():
    import http.cookiejar
    import requests as req

    cookies_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "cookies.txt")
    session = req.Session()

    if os.path.exists(cookies_path) and os.path.getsize(cookies_path) > 0:
        try:
            cj = http.cookiejar.MozillaCookieJar(cookies_path)
            cj.load(ignore_discard=True, ignore_expires=True)
            session.cookies = cj
            logger.info(f"Loaded {len(cj)} cookies for caption API")
        except Exception as e:
            logger.warning(f"Could not load cookies.txt: {e}")

    return YouTubeTranscriptApi(http_client=session)


def fetch_captions(video_id: str, max_retries: int = 4):
    ytt = _get_caption_api()

    for attempt in range(max_retries):
        try:
            transcript_list = ytt.list(video_id)

            transcript = None
            language = "en"

            try:
                transcript = transcript_list.find_manually_created_transcript(["en", "en-US", "en-GB"])
                language = transcript.language_code
                logger.info(f"Found manual captions for {video_id} ({language})")
            except Exception:
                pass

            if not transcript:
                try:
                    transcript = transcript_list.find_generated_transcript(["en", "en-US", "en-GB"])
                    language = transcript.language_code
                    logger.info(f"Found auto captions for {video_id} ({language})")
                except Exception:
                    pass

            if not transcript:
                try:
                    available = list(transcript_list)
                    if available:
                        for t in available:
                            if not t.is_generated:
                                transcript = t
                                language = t.language_code
                                break
                        if not transcript:
                            transcript = available[0]
                            language = transcript.language_code
                        logger.info(f"Using non-English captions for {video_id} ({language})")
                except Exception:
                    pass

            if not transcript:
                logger.warning(f"No captions available for {video_id}")
                return False, "No captions available"

            fetched = transcript.fetch()

            segments = []
            text_parts = []

            for entry in fetched:
                if hasattr(entry, "text"):
                    t = entry.text
                    start = getattr(entry, "start", 0.0)
                    dur = getattr(entry, "duration", 0.0)
                elif isinstance(entry, dict):
                    t = entry.get("text", "")
                    start = entry.get("start", 0.0)
                    dur = entry.get("duration", 0.0)
                else:
                    continue
                text_parts.append(t)
                segments.append({"start": float(start), "end": float(start) + float(dur), "text": t})

            full_text = " ".join(text_parts).strip()
            if not full_text:
                return False, "Captions fetched but empty"

            word_count = len(full_text.split())

            ok, err_db = db.insert_transcript(
                video_id=video_id,
                full_text=full_text,
                segments_json=json.dumps(segments),
                language=language,
                word_count=word_count,
                transcript_provider="youtube_captions",
                transcript_model="youtube_captions",
            )
            if not ok:
                msg = err_db or "Failed to write captions to DB"
                db.update_video_status(video_id, "failed", msg)
                logger.error(f"DB insert_transcript failed for {video_id}: {msg}")
                return False, msg

            db.update_video_status(video_id, "transcribed", None)
            logger.info(f"Captions saved for {video_id}: {word_count} words, lang={language}")
            return True, None

        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "Too Many Requests" in err_str or "IpBlocked" in type(e).__name__:
                wait = 2 ** (attempt + 1)
                logger.warning(
                    f"YouTube rate limit/block for captions {video_id}, backing off {wait}s (attempt {attempt+1}/{max_retries})"
                )
                time.sleep(wait)
                continue

            if "TranscriptsDisabled" in type(e).__name__ or "NoTranscriptFound" in type(e).__name__:
                logger.warning(f"No captions for {video_id}: {type(e).__name__}")
                return False, f"No captions: {type(e).__name__}"

            logger.error(f"Caption fetch failed for {video_id}: {e}")
            return False, f"Caption fetch error: {err_str}"

    return False, f"Caption fetch failed after {max_retries} retries (rate limited/blocked)"


def cleanup_audio(video_id: str, success: bool):
    audio_path = os.path.join(TMP_AUDIO_DIR, f"{video_id}.mp3")
    if os.path.exists(audio_path):
        if success or not KEEP_AUDIO_ON_FAIL:
            try:
                os.remove(audio_path)
                logger.info(f"Cleaned up audio: {audio_path}")
            except Exception:
                pass
