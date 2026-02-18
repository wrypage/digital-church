import re
import json
import math
import logging
from datetime import datetime, timedelta
from engine.config import load_theology_config
from engine import db

logger = logging.getLogger("digital_pulpit")


def normalize_text(text):
    """
    Normalize text for theological keyword matching.
    Handles possessives and punctuation properly.
    """
    if not text:
        return ""

    text = text.lower()
    # Remove possessives before word boundaries (God's → God)
    text = re.sub(r"'s\b", "", text)
    # Remove other punctuation
    text = re.sub(r"[^\w\s]", " ", text)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


def count_category_matches(text, keywords):
    count = 0
    for kw in keywords:
        pattern = r"\b" + re.escape(kw.lower()) + r"\b"
        count += len(re.findall(pattern, text))
    return count


def calculate_theological_density(category_scores, word_count, config):
    if word_count < config.get("density_normalization", {}).get("min_word_count", 100):
        return 0.0
    total_matches = sum(category_scores.values())
    return round((total_matches / word_count) * 1000, 4)


def calculate_axis_score(category_scores, positive_cat, negative_cat):
    pos = category_scores.get(positive_cat, 0)
    neg = category_scores.get(negative_cat, 0)
    total = pos + neg
    if total == 0:
        return 0.0
    return round((pos - neg) / total, 4)


def analyze_transcript(video_id):
    """
    Analyze a transcript for theological content and compute density/axis scores.
    Returns True on success, False on failure.
    """
    config = load_theology_config()
    if not config:
        logger.error("Theology config not loaded")
        return False

    transcript = db.get_transcript(video_id)
    if not transcript:
        logger.warning(f"No transcript for {video_id}")
        return False

    # Safety checks for transcript data
    full_text = transcript.get("full_text", "")
    if not full_text or not full_text.strip():
        logger.warning(f"Empty transcript text for {video_id}")
        return False

    # Get word count from transcript, or calculate as fallback
    word_count = transcript.get("word_count", 0)
    if word_count == 0:
        word_count = len(full_text.split())
        if word_count == 0:
            logger.warning(f"Zero word count for {video_id}")
            return False

    normalized = normalize_text(full_text)

    categories = config.get("theological_categories", {})
    category_scores = {}
    for cat_name, cat_data in categories.items():
        keywords = cat_data.get("keywords", [])
        category_scores[cat_name] = count_category_matches(normalized, keywords)

    density = calculate_theological_density(category_scores, word_count, config)

    drift_axes = config.get("drift_axes", {})
    grace_effort = calculate_axis_score(
        category_scores,
        drift_axes.get("grace_vs_effort", {}).get("positive", "grace"),
        drift_axes.get("grace_vs_effort", {}).get("negative", "effort")
    )
    hope_fear = calculate_axis_score(
        category_scores,
        drift_axes.get("hope_vs_fear", {}).get("positive", "hope"),
        drift_axes.get("hope_vs_fear", {}).get("negative", "fear")
    )
    doctrine_exp = calculate_axis_score(
        category_scores,
        drift_axes.get("doctrine_vs_experience", {}).get("positive", "doctrine"),
        drift_axes.get("doctrine_vs_experience", {}).get("negative", "experience")
    )
    scripture_story = calculate_axis_score(
        category_scores,
        drift_axes.get("scripture_vs_story", {}).get("positive", "scripture_reference"),
        drift_axes.get("scripture_vs_story", {}).get("negative", "story")
    )

    sorted_cats = sorted(category_scores.items(), key=lambda x: x[1], reverse=True)
    top_cats = json.dumps([c[0] for c in sorted_cats[:5]])

    db.insert_brain_result(
        video_id, density, grace_effort, hope_fear,
        doctrine_exp, scripture_story, top_cats, json.dumps(category_scores)
    )

    logger.info(f"Analyzed {video_id}: density={density}, grace/effort={grace_effort}")
    return True


def compute_zscore(values):
    if len(values) < 2:
        return [0.0] * len(values)
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    std = math.sqrt(variance) if variance > 0 else 1.0
    return [round((v - mean) / std, 4) for v in values]


def generate_weekly_drift():
    """
    Generate weekly drift reports for each channel.
    Computes z-scores for theological axes to detect drift over time.
    """
    config = load_theology_config()
    if not config:
        logger.error("Theology config not loaded")
        return False

    today = datetime.utcnow().date()
    week_start = today - timedelta(days=today.weekday() + 7)
    week_end = week_start + timedelta(days=6)

    # FIX: Get only results from this specific week, not all results ever
    results = db.get_brain_results_for_week(week_start, week_end)
    if not results:
        logger.warning(f"No brain results for week {week_start} to {week_end}")
        return False

    channels = {}
    for r in results:
        cid = r.get("channel_id", "unknown")
        if cid not in channels:
            channels[cid] = []
        channels[cid].append(r)

    for channel_id, ch_results in channels.items():
        if len(ch_results) < 1:
            continue

        # Extract scores with safety checks
        densities = [r.get("theological_density", 0.0) for r in ch_results if r.get("theological_density") is not None]
        grace_scores = [r.get("grace_vs_effort", 0.0) for r in ch_results if r.get("grace_vs_effort") is not None]
        hope_scores = [r.get("hope_vs_fear", 0.0) for r in ch_results if r.get("hope_vs_fear") is not None]
        doctrine_scores = [r.get("doctrine_vs_experience", 0.0) for r in ch_results if r.get("doctrine_vs_experience") is not None]
        scripture_scores = [r.get("scripture_vs_story", 0.0) for r in ch_results if r.get("scripture_vs_story") is not None]

        # Skip if we don't have enough data
        if not densities or not grace_scores:
            logger.warning(f"Insufficient data for channel {channel_id} in week {week_start}")
            continue

        grace_z = compute_zscore(grace_scores)
        hope_z = compute_zscore(hope_scores)
        doctrine_z = compute_zscore(doctrine_scores)
        scripture_z = compute_zscore(scripture_scores)

        avg_density = round(sum(densities) / len(densities), 4)
        latest_grace_z = grace_z[-1] if grace_z else 0
        latest_hope_z = hope_z[-1] if hope_z else 0
        latest_doctrine_z = doctrine_z[-1] if doctrine_z else 0
        latest_scripture_z = scripture_z[-1] if scripture_z else 0

        report = {
            "channel_id": channel_id,
            "channel_name": ch_results[0].get("channel_name", ""),
            "sample_size": len(ch_results),
            "avg_density": avg_density,
            "drift_summary": {
                "grace_vs_effort": latest_grace_z,
                "hope_vs_fear": latest_hope_z,
                "doctrine_vs_experience": latest_doctrine_z,
                "scripture_vs_story": latest_scripture_z,
            }
        }

        db.insert_weekly_drift(
            str(week_start), str(week_end), channel_id,
            avg_density, latest_grace_z, latest_hope_z,
            latest_doctrine_z, latest_scripture_z,
            len(ch_results), json.dumps(report)
        )

    logger.info("Weekly drift report generated")
    return True


def run_brain():
    run_id = db.create_run("brain")
    logger.info(f"Starting Brain run #{run_id}")
    count = 0

    try:
        unanalyzed = db.get_transcribed_videos_without_analysis()
        logger.info(f"Found {len(unanalyzed)} transcripts to analyze")

        for video in unanalyzed:
            db.update_video_status(video["video_id"], "queued_for_brain")
            success = analyze_transcript(video["video_id"])
            if success:
                count += 1

        generate_weekly_drift()
        db.finish_run(run_id, "completed", count, 0, f"Analyzed {count} transcripts")

    except Exception as e:
        logger.error(f"Brain run failed: {e}", exc_info=True)
        db.finish_run(run_id, "failed", count, 0, str(e))

    logger.info(f"Brain run #{run_id} finished: {count} analyzed")
    return run_id
