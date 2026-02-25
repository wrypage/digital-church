import json
import logging
from datetime import datetime, timedelta
from engine.config import load_theology_config
from engine import db
from engine.climate_agenda import generate_climate_agenda

logger = logging.getLogger("digital_pulpit")


def compute_affinity_score(brain_result, avatar_config, category_scores):
    affinity_cats = avatar_config.get("affinity_categories", [])
    score = 0.0
    for cat in affinity_cats:
        score += category_scores.get(cat, 0)
    return score


def score_sentence_quality(sentence, avatar_config, category_scores):
    """
    Score a sentence for theological relevance and quality.
    Higher scores indicate better quotes.
    """
    if not sentence or len(sentence) < 30:
        return 0.0

    score = 0.0
    normalized = sentence.lower()

    affinity_cats = avatar_config.get("affinity_categories", [])
    for cat in affinity_cats:
        score += category_scores.get(cat, 0) * 0.5

    length = len(sentence)
    if 100 <= length <= 200:
        score += 2.0
    elif 200 < length <= 300:
        score += 1.0
    elif length < 50:
        score -= 1.0

    theological_indicators = ["god", "christ", "jesus", "lord", "spirit", "faith", "grace", "gospel", "scripture", "biblical"]
    for word in theological_indicators:
        if word in normalized:
            score += 0.5

    return score


def select_quotes_for_avatar(avatar_key, avatar_config, brain_results_with_transcripts):
    """
    Select best quotes for an avatar from brain results.
    Uses affinity scoring + sentence quality metrics.
    """
    if not brain_results_with_transcripts:
        logger.warning(f"No brain results available for avatar {avatar_key}")
        return []

    scored = []
    for item in brain_results_with_transcripts:
        if not item.get("brain") or not item.get("transcript"):
            continue

        raw_json = item["brain"].get("raw_scores_json", "{}")
        raw = json.loads(raw_json) if raw_json else {}
        affinity = compute_affinity_score(item["brain"], avatar_config, raw)
        scored.append((affinity, item, raw))

    scored.sort(key=lambda x: x[0], reverse=True)

    quotes = []
    for score, item, raw_scores in scored[:5]:
        text = item["transcript"].get("full_text", "")
        if not text or not text.strip():
            continue

        sentences = []
        for s in text.replace("!", ".").replace("?", ".").split("."):
            s = s.strip()
            if len(s) > 30:
                sentences.append(s)

        if not sentences:
            continue

        sentence_scores = []
        for sent in sentences[:30]:
            sent_score = score_sentence_quality(sent, avatar_config, raw_scores)
            sentence_scores.append((sent_score, sent))

        if sentence_scores:
            sentence_scores.sort(key=lambda x: x[0], reverse=True)
            best_sentence = sentence_scores[0][1]

            quotes.append({
                "video_id": item["brain"].get("video_id", ""),
                "title": item["brain"].get("title", "Unknown"),
                "channel": item["brain"].get("channel_name", "Unknown"),
                "quote": best_sentence[:300],
                "affinity_score": round(score, 2),
            })

        if len(quotes) >= 3:
            break

    return quotes


def generate_avatar_section(avatar_key, avatar_config, quotes, briefing=None):
    """
    Generate markdown section for an avatar with:
      - a targeted prompt anchored in "What are our pastors trying to tell us?"
      - 0–3 signal receipts (quotes)
      - fallback handling
    """
    name = avatar_config.get("name", f"Avatar {avatar_key}")
    tradition = avatar_config.get("tradition", "Unknown tradition")
    voice = avatar_config.get("voice", "Neutral")
    fallback = avatar_config.get("fallback_intro", "")

    lines = []
    lines.append(f"## {name} ({tradition})")
    lines.append(f"*Voice: {voice}*")
    lines.append("")

    # Targeted prompt (new)
    if briefing:
        lines.append("**Targeted prompt (anchored inquiry):**")
        lines.append("- What are our pastors trying to tell us in this window, through your lens?")
        if briefing.get("top_message"):
            lines.append(f"- Isolate the signal being pressed: {briefing['top_message']}")
        if briefing.get("top_warning"):
            lines.append(f"- Name what this repeated warning suggests about the moment: {briefing['top_warning']}")
        if briefing.get("top_cta"):
            lines.append(f"- Translate the main call-to-action into practical next steps: {briefing['top_cta']}")
        lines.append("")

    # Receipts
    if quotes:
        lines.append("**Signal receipts:**")
        lines.append(f"\"{quotes[0]['quote']}\"")
        lines.append(f"— From \"{quotes[0]['title']}\" ({quotes[0]['channel']})")
        lines.append("")

        if len(quotes) > 1:
            lines.append("Additional signals:")
            for q in quotes[1:]:
                quote_preview = q["quote"][:150]
                if len(q["quote"]) > 150:
                    quote_preview += "..."
                lines.append(f"  - \"{quote_preview}\" — {q['channel']}")
            lines.append("")
    else:
        if fallback:
            lines.append(fallback)
        else:
            lines.append(f"*No sermons found matching {name}'s theological profile this period.*")
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
        if not avatars:
            logger.warning("No avatars configured in theology config")
            db.finish_run(run_id, "failed", 0, 0, "No avatars in config")
            return run_id

        logger.info(f"Loaded {len(avatars)} avatars from config")

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

        logger.info(f"Found {len(brain_results)} brain results")

        items = []
        for br in brain_results:
            video_id = br.get("video_id")
            if not video_id:
                logger.warning("Brain result missing video_id, skipping")
                continue

            transcript = db.get_transcript(video_id)
            if transcript:
                items.append({"brain": br, "transcript": transcript})
            else:
                logger.debug(f"No transcript found for {video_id}")

        if not items:
            logger.warning("No transcripts matched to brain results")
            db.finish_run(run_id, "failed", 0, 0, "No matching transcripts")
            return run_id

        logger.info(f"Matched {len(items)} brain results with transcripts")

        # Build Elias anchor from intent climate
        agenda = generate_climate_agenda(days=30, limit=240, limit_each=2)
        intent_pack = agenda.get("intent_climate_v2") or {}
        intent = intent_pack.get("climate_v2") if isinstance(intent_pack, dict) else None

        top_message = ""
        top_warning = ""
        top_cta = ""

        if intent:
            pm = intent.get("primary_messages_being_pressed") or []
            wr = intent.get("warnings_repeated") or []
            ca = intent.get("calls_to_action_most_urged") or []
            top_message = (pm[0].get("message") if pm else "") or ""
            top_warning = (wr[0].get("warning") if wr else "") or ""
            top_cta = (ca[0].get("action") if ca else "") or ""

        today = datetime.utcnow().date()
        week_start = today - timedelta(days=today.weekday())
        week_end = week_start + timedelta(days=6)

        script_parts = []
        script_parts.append("# The Digital Pulpit — Weekly Script")
        script_parts.append(f"## Week of {week_start} to {week_end}")
        script_parts.append(f"*Generated: {datetime.utcnow().isoformat()}*")
        script_parts.append("")
        script_parts.append("---")
        script_parts.append("")

        # Elias session anchor (new)
        script_parts.append("## Elias — Session Anchor")
        script_parts.append("**Guiding question:** What are our pastors trying to tell us?")
        script_parts.append("")
        if intent:
            tw = intent.get("time_window") or {}
            msg = intent.get("the_message_this_window") or {}
            script_parts.append(f"Time window: {tw.get('key','')}")
            script_parts.append(f"Channels: {tw.get('channels_included','')} | Sermons: {tw.get('sermons_included','')}")
            script_parts.append("")
            if msg.get("headline"):
                script_parts.append(f"**Headline:** {msg.get('headline')}")
            if msg.get("summary"):
                script_parts.append(f"{msg.get('summary')}")
            script_parts.append("")

            def _block(title: str, rows, key):
                script_parts.append(f"### {title}")
                if not rows:
                    script_parts.append("(none detected in this window)")
                    script_parts.append("")
                    return
                for r in rows[:3]:
                    val = (r.get(key) or "").strip()
                    if not val:
                        continue
                    script_parts.append(f"- {val}")
                    ev = r.get("evidence") or []
                    if ev:
                        ex = (ev[0].get("excerpt") or "").strip()
                        if ex:
                            script_parts.append(f"  - \"{ex}\"")
                            script_parts.append(f"  - ({ev[0].get('channel_name','')} — {ev[0].get('published_at','')})")
                script_parts.append("")

            _block("Primary messages being pressed", intent.get("primary_messages_being_pressed") or [], "message")
            _block("Warnings repeated", intent.get("warnings_repeated") or [], "warning")
            _block("Encouragements amplified", intent.get("encouragements_amplified") or [], "encouragement")
            _block("Calls to action most urged", intent.get("calls_to_action_most_urged") or [], "action")

            qs = intent.get("questions_for_leaders") or []
            if qs:
                script_parts.append("### Questions for leaders")
                for q in qs[:5]:
                    script_parts.append(f"- {q}")
                script_parts.append("")
        else:
            script_parts.append("No intent climate data available for this window.")
            script_parts.append("")

        script_parts.append("---")
        script_parts.append("")

        avatar_assignments = {}
        source_ids = set()
        avatars_with_quotes = 0
        total_quotes = 0

        # Preferred order to keep the “inquiry loop” stable
        preferred_order = ["sully", "aris", "noni", "elena", "tio"]
        ordered_keys = [k for k in preferred_order if k in avatars] + [k for k in avatars.keys() if k not in preferred_order]

        briefing = {
            "top_message": top_message,
            "top_warning": top_warning,
            "top_cta": top_cta,
        }

        for avatar_key in ordered_keys:
            avatar_config = avatars.get(avatar_key, {})
            try:
                quotes = select_quotes_for_avatar(avatar_key, avatar_config, items)
                section = generate_avatar_section(avatar_key, avatar_config, quotes, briefing=briefing)
                script_parts.append(section)
                script_parts.append("---")
                script_parts.append("")

                avatar_assignments[avatar_key] = [q["video_id"] for q in quotes]
                for q in quotes:
                    source_ids.add(q["video_id"])

                if quotes:
                    avatars_with_quotes += 1
                    total_quotes += len(quotes)
                    logger.info(f"Avatar {avatar_key}: {len(quotes)} quotes selected")
                else:
                    logger.info(f"Avatar {avatar_key}: using fallback (no matching quotes)")

            except Exception as e:
                logger.error(f"Failed to process avatar {avatar_key}: {e}", exc_info=True)
                fallback = avatar_config.get("fallback_intro", "Error generating section")
                script_parts.append(f"## {avatar_config.get('name', avatar_key)}")
                script_parts.append(fallback)
                script_parts.append("---")
                script_parts.append("")

        full_script = "\n".join(script_parts)

        db.insert_assembly_script(
            str(week_start), str(week_end), full_script,
            json.dumps(avatar_assignments),
            ",".join(source_ids)
        )

        summary = f"{len(avatars)} avatars ({avatars_with_quotes} with quotes, {total_quotes} total quotes)"
        db.finish_run(run_id, "completed", len(items), 0, summary)
        logger.info(f"Assembly run #{run_id} complete: {summary}")

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
        name = av.get("name", f"Avatar {key}")
        tradition = av.get("tradition", "Unknown tradition")
        voice = av.get("voice", "Neutral")
        fallback_intro = av.get("fallback_intro", f"*{name} awaits new sermon content.*")

        parts.append(f"## {name} ({tradition})")
        parts.append(f"*Voice: {voice}*")
        parts.append("")
        parts.append(fallback_intro)
        parts.append("")
        parts.append("---")
        parts.append("")

    return "\n".join(parts)