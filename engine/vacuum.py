import logging
from engine.config import (
    load_channels_csv, MAX_MINUTES_PER_RUN, MAX_VIDEOS_PER_RUN
)
from engine import db
from engine.youtube import resolve_channel_id, discover_videos
from engine.transcription import download_audio, transcribe_audio, cleanup_audio

logger = logging.getLogger("digital_pulpit")


def run_vacuum():
    run_id = db.create_run("vacuum")
    logger.info(f"Starting Vacuum run #{run_id}")
    logger.info(f"Limits: max_videos={MAX_VIDEOS_PER_RUN}, max_minutes={MAX_MINUTES_PER_RUN}")

    total_videos = 0
    total_minutes = 0.0
    notes_parts = []

    try:
        channels = load_channels_csv()
        if not channels:
            msg = "No channels found in channels.csv"
            logger.warning(msg)
            notes_parts.append(msg)
            db.finish_run(run_id, "completed", 0, 0, msg)
            return run_id

        for ch in channels:
            if total_videos >= MAX_VIDEOS_PER_RUN:
                msg = f"MAX_VIDEOS_PER_RUN ({MAX_VIDEOS_PER_RUN}) reached"
                logger.warning(msg)
                notes_parts.append(msg)
                break
            if total_minutes >= MAX_MINUTES_PER_RUN:
                msg = f"MAX_MINUTES_PER_RUN ({MAX_MINUTES_PER_RUN}) reached"
                logger.warning(msg)
                notes_parts.append(msg)
                break

            channel_id, method = resolve_channel_id(ch)
            if not channel_id:
                notes_parts.append(f"Failed to resolve: {ch['name']}")
                continue

            db.upsert_channel(channel_id, ch["name"], ch["url"], method)

            videos = discover_videos(channel_id)
            logger.info(f"  {ch['name']}: {len(videos)} videos discovered")

            for v in videos:
                if total_videos >= MAX_VIDEOS_PER_RUN:
                    break
                if total_minutes >= MAX_MINUTES_PER_RUN:
                    break

                video_id = v["video_id"]
                duration_min = v["duration_seconds"] / 60.0

                audio_path = download_audio(video_id)
                if not audio_path:
                    db.update_video_status(video_id, "failed", "Audio download failed")
                    notes_parts.append(f"Download failed: {video_id}")
                    continue

                db.update_video_status(video_id, "audio_downloaded")

                success = transcribe_audio(video_id, audio_path)
                cleanup_audio(video_id, success)

                if success:
                    total_videos += 1
                    total_minutes += duration_min
                else:
                    notes_parts.append(f"Transcription failed: {video_id}")

        status = "completed"
        notes = "; ".join(notes_parts) if notes_parts else "All OK"

    except Exception as e:
        status = "failed"
        notes = f"Run failed: {str(e)}"
        logger.error(notes, exc_info=True)

    db.finish_run(run_id, status, total_videos, round(total_minutes, 2), notes)
    logger.info(f"Vacuum run #{run_id} finished: {status}, {total_videos} videos, {total_minutes:.1f} minutes")
    return run_id
