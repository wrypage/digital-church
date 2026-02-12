import json
import logging
from datetime import datetime, timedelta
from engine.config import load_theology_config
from engine import db

logger = logging.getLogger("digital_pulpit")


def compute_affinity_score(brain_result, avatar_config, category_scores):
    affinity_cats = avatar_config.get("affinity_categories", [])
    score = 0.0
    for cat in affinity_cats:
        score += category_scores.get(cat, 0)
    return score


def select_quotes_for_avatar(avatar_key, avatar_config, brain_results_with_transcripts):
    scored = []
    for item in brain_results_with_transcripts:
        raw = json.loads(item["brain"]["raw_scores_json"]) if item["brain"]["raw_scores_json"] else {}
        affinity = compute_affinity_score(item["brain"], avatar_config, raw)
        scored.append((affinity, item))

    scored.sort(key=lambda x: x[0], reverse=True)

    quotes = []
    for score, item in scored[:3]:
        text = item["transcript"]["full_text"]
        sentences = [s.strip() for s in text.replace("!", ".").replace("?", ".").split(".") if len(s.strip()) > 30]
        if sentences:
            best = max(sentences[:20], key=lambda s: len(s)) if sentences else sentences[0]
            quotes.append({
                "video_id": item["brain"]["video_id"],
                "title": item["brain"].get("title", ""),
                "channel": item["brain"].get("channel_name", ""),
                "quote": best[:300],
                "affinity_score": round(score, 2),
            })

    return quotes


def generate_avatar_section(avatar_key, avatar_config, quotes):
    name = avatar_config["name"]
    tradition = avatar_config["tradition"]
    voice = avatar_config["voice"]
    fallback = avatar_config.get("fallback_intro", "")

    lines = []
    lines.append(f"## {name} ({tradition})")
    lines.append(f"*Voice: {voice}*")
    lines.append("")

    if quotes:
        lines.append(f'"{quotes[0]["quote"]}"')
        lines.append(f'— From "{quotes[0]["title"]}" ({quotes[0]["channel"]})')
        lines.append("")
        if len(quotes) > 1:
            lines.append("Additional signals:")
            for q in quotes[1:]:
                lines.append(f'  - "{q["quote"][:150]}..." — {q["channel"]}')
            lines.append("")
    else:
        lines.append(fallback)
        lines.append("")

    return "\n".join(lines)


def run_assembly():
    run_id = db.create_run("assembly")
    logger.info(f"Starting Assembly run #{run_id}")

    try:
        config = load_theology_config()
        if not config:
            db.finish_run(run_id, "failed", 0, 0, "Config not loaded")
            return run_id

        avatars = config.get("avatars", {})
        brain_results = db.get_all_brain_results()

        if not brain_results:
            logger.warning("No brain results available for assembly")
            script = generate_fallback_script(avatars)
            today = datetime.utcnow().date()
            week_start = today - timedelta(days=today.weekday())
            week_end = week_start + timedelta(days=6)
            db.insert_assembly_script(
                str(week_start), str(week_end), script,
                json.dumps({"note": "fallback - no data"}), ""
            )
            db.finish_run(run_id, "completed", 0, 0, "Fallback script generated (no data)")
            return run_id

        items = []
        for br in brain_results:
            transcript = db.get_transcript(br["video_id"])
            if transcript:
                items.append({"brain": br, "transcript": transcript})

        if not items:
            logger.warning("No transcripts matched to brain results")
            db.finish_run(run_id, "failed", 0, 0, "No matching transcripts")
            return run_id

        today = datetime.utcnow().date()
        week_start = today - timedelta(days=today.weekday())
        week_end = week_start + timedelta(days=6)

        script_parts = []
        script_parts.append(f"# The Digital Pulpit — Weekly Script")
        script_parts.append(f"## Week of {week_start} to {week_end}")
        script_parts.append(f"*Generated: {datetime.utcnow().isoformat()}*")
        script_parts.append("")
        script_parts.append("---")
        script_parts.append("")

        avatar_assignments = {}
        source_ids = set()

        for avatar_key, avatar_config in avatars.items():
            quotes = select_quotes_for_avatar(avatar_key, avatar_config, items)
            section = generate_avatar_section(avatar_key, avatar_config, quotes)
            script_parts.append(section)
            script_parts.append("---")
            script_parts.append("")
            avatar_assignments[avatar_key] = [q["video_id"] for q in quotes]
            for q in quotes:
                source_ids.add(q["video_id"])

        full_script = "\n".join(script_parts)

        db.insert_assembly_script(
            str(week_start), str(week_end), full_script,
            json.dumps(avatar_assignments),
            ",".join(source_ids)
        )

        db.finish_run(run_id, "completed", len(items), 0, f"Script generated with {len(avatars)} avatars")
        logger.info(f"Assembly run #{run_id} complete")

    except Exception as e:
        logger.error(f"Assembly run failed: {e}", exc_info=True)
        db.finish_run(run_id, "failed", 0, 0, str(e))

    return run_id


def generate_fallback_script(avatars):
    parts = []
    parts.append("# The Digital Pulpit — Weekly Script (Fallback)")
    parts.append(f"*Generated: {datetime.utcnow().isoformat()}*")
    parts.append("")
    parts.append("*No sermon data available this week. Fallback introductions below.*")
    parts.append("")
    parts.append("---")
    parts.append("")

    for key, av in avatars.items():
        parts.append(f"## {av['name']} ({av['tradition']})")
        parts.append(f"*Voice: {av['voice']}*")
        parts.append("")
        parts.append(av.get("fallback_intro", "..."))
        parts.append("")
        parts.append("---")
        parts.append("")

    return "\n".join(parts)
