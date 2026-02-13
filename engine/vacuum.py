import logging
import os
from engine.config import load_channels_csv, MAX_MINUTES_PER_RUN, MAX_VIDEOS_PER_RUN
from engine import db
from engine.youtube import resolve_channel_id, discover_videos
from engine.transcription import download_audio, transcribe_audio, cleanup_audio

logger = logging.getLogger("digital_pulpit")


def _safe_int(value, default=0):
    try:
        if value is None:
            return default
        if isinstance(value, bool):
            return default
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        s = str(value).strip()
        if not s:
            return default
        return int(float(s))
    except Exception:
        return default


def run_vacuum():
    run_id = db.create_run("vacuum")
    logger.info(f"Starting Vacuum run #{run_id}")
    logger.info(
        f"Limits: max_videos={MAX_VIDEOS_PER_RUN}, max_minutes={MAX_MINUTES_PER_RUN}"
    )

    # Filters
    min_duration_seconds = _safe_int(
        os.environ.get("MIN_DURATION_SECONDS", "480"), 480)  # 8 minutes
    max_duration_seconds = _safe_int(
        os.environ.get("MAX_VIDEO_DURATION_SECONDS", "3600"),
        3600)  # 60 minutes

    total_videos = 0
    total_minutes = 0.0
    notes_parts = []

    status = "completed"
    notes = "All OK"

    try:
        channels = load_channels_csv()
        if not channels:
            notes = "No channels found in channels.csv"
            logger.warning(notes)
            db.finish_run(run_id, "completed", 0, 0.0, notes)
            return {
                "ok": True,
                "run_type": "vacuum",
                "run_id": run_id,
                "notes": notes
            }

        for ch in channels:
            if total_videos >= MAX_VIDEOS_PER_RUN:
                notes_parts.append(
                    f"MAX_VIDEOS_PER_RUN ({MAX_VIDEOS_PER_RUN}) reached")
                break
            if total_minutes >= float(MAX_MINUTES_PER_RUN):
                notes_parts.append(
                    f"MAX_MINUTES_PER_RUN ({MAX_MINUTES_PER_RUN}) reached")
                break

            channel_id, method = resolve_channel_id(ch)
            if not channel_id:
                notes_parts.append(
                    f"Failed to resolve: {ch.get('name','(unknown)')}")
                continue

            db.upsert_channel(channel_id, ch.get("name", ""),
                              ch.get("url", ""), method)

            videos = discover_videos(channel_id)
            logger.info(
                f"  {ch.get('name','(unknown)')}: {len(videos)} videos discovered"
            )

            for v in videos:
                if total_videos >= MAX_VIDEOS_PER_RUN:
                    break

                video_id = v.get("video_id")
                if not video_id:
                    continue

                duration_seconds = _safe_int(v.get("duration_seconds"), 0)
                duration_min = duration_seconds / 60.0

                # Insert video row (dedupe safe)
                db.insert_or_ignore_video(
                    video_id=video_id,
                    channel_id=channel_id,
                    title=v.get("title", "") or "",
                    published_at=v.get("published_at", "") or "",
                    duration_seconds=duration_seconds,
                    status="discovered",
                    error_message=None,
                )

                # Skip shorts/clips
                if duration_seconds and duration_seconds < min_duration_seconds:
                    db.update_video_status(video_id, "skipped",
                                           f"Too short ({duration_seconds}s)")
                    continue

                # Skip overly long services (cost / time)
                if duration_seconds and duration_seconds > max_duration_seconds:
                    db.update_video_status(video_id, "skipped",
                                           f"Too long ({duration_seconds}s)")
                    notes_parts.append(
                        f"Skipped too long: {video_id} ({duration_seconds}s)")
                    continue

                # Preflight budget check BEFORE starting work
                if (total_minutes + duration_min) > float(MAX_MINUTES_PER_RUN):
                    notes_parts.append(
                        f"MAX_MINUTES_PER_RUN ({MAX_MINUTES_PER_RUN}) reached")
                    break

                db.update_video_status(video_id, "downloading_audio", None)

                audio_path = download_audio(video_id)
                if not audio_path:
                    db.update_video_status(video_id, "failed",
                                           "Audio download failed")
                    notes_parts.append(f"Download failed: {video_id}")
                    continue

                db.update_video_status(video_id, "audio_downloaded", None)
                db.update_video_status(video_id, "transcribing", None)

                success, err = transcribe_audio(video_id, audio_path)
                cleanup_audio(video_id, success)

                if success:
                    total_videos += 1
                    total_minutes += duration_min
                    db.update_video_status(video_id, "transcribed", None)
                else:
                    msg = err or "Transcription failed"
                    db.update_video_status(video_id, "failed", msg)
                    notes_parts.append(
                        f"Transcription failed: {video_id} ({msg})")

        notes = "; ".join(notes_parts) if notes_parts else "All OK"

    except Exception as e:
        status = "failed"
        notes = f"Run failed: {type(e).__name__}: {str(e)}"
        logger.error(notes, exc_info=True)

    db.finish_run(run_id, status, total_videos, round(total_minutes, 2), notes)
    return {
        "ok": status == "completed",
        "run_type": "vacuum",
        "run_id": run_id,
        "notes": notes
    }
