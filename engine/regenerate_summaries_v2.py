#!/usr/bin/env python3
"""
engine/regenerate_summaries_v2.py

Regenerate summaries for specific videos using Summary Generator V2 prompt.
Updates database and triggers brain recomputation.

Usage:
  python -m engine.regenerate_summaries_v2 --db db/digital_pulpit.db
"""

import argparse
import json
import os
import sqlite3
import sys
from typing import List, Dict, Any

# Import OpenAI client
try:
    from openai import OpenAI
except ImportError:
    print("Error: openai package not installed. Run: pip install openai")
    sys.exit(1)


# ---------------------------
# Summary Generator V2 Prompt
# ---------------------------

SUMMARY_GENERATOR_V2_SYSTEM = """You are generating a structured analytical summary of a Christian sermon
for downstream theological and pastoral analysis.
This summary will be used as analytic substrate for automated signal detection.
Accuracy and signal preservation are more important than literary style."""

SUMMARY_GENERATOR_V2_USER = """
INPUT TRANSCRIPT:
{transcript}

RULES:
1. Do NOT invent tone, claims, or emotional content not present in the transcript.
2. Do NOT over-compress grace language, hope language, warning language,
   or encouragement language.
3. If the preacher expresses comfort, assurance, joy, or encouragement —
   preserve it explicitly.
4. If the preacher expresses warning, urgency, correction, or rebuke —
   preserve it explicitly.
5. Preserve explicit scripture references (book + chapter + verse when available).
6. If named theologians, movements, or intellectual opponents are referenced
   (e.g., Gnosticism, Dallas Willard), include them.
7. Exclude:
   - Announcements
   - Offering instructions
   - Housekeeping
   - Repeated filler prayer language
   - Casual crowd interaction unless it materially advances the thesis
8. Do not artificially balance theological themes. Preserve what is actually present.
9. Pay special attention to the sermon's closing movement (final 10–15%).
   If the preacher lands the sermon with grace, assurance, invitation,
   hope, relational appeal, or direct gospel proclamation, preserve
   that closing movement with proportional weight in the summary.
   Do not compress the landing into a single generic sentence.
10. Preserve rhetorical emphasis proportionally.
    If a theme is clearly emphasized repeatedly near the conclusion,
    reflect that emphasis in the summary with multiple sentences
    rather than a single generic paraphrase.
11. Distinguish between the content of the scripture text being preached
    and the emotional posture of the preacher toward the congregation.
    A sermon on a doctrinal text may be delivered with warmth and pastoral
    care. Capture both the theological content AND the preacher's emotional
    posture separately.

REQUIRED OUTPUT FORMAT:

### Thesis
1–3 sentences describing the central theological claim of the sermon.

### Pastoral Burden
One sentence beginning with:
"The pastor is pressing the congregation to..."
This must clearly describe what the preacher is trying to produce
in the listener this week.

### Emotional Register
Describe the dominant tone of the sermon in 2–4 sentences.
Explicitly name whether the tone is hopeful, urgent, corrective,
comforting, exhortational, celebratory, warning-heavy, or mixed.
If the sermon is primarily exhortational but contains significant
moments of comfort or assurance, name both.
If warmth or encouragement is present, state it directly.
If rebuke or warning is dominant, state it directly.

### Key Movements
3–7 bullet points summarizing the progression of the argument.
Each bullet must describe a movement in reasoning, not a story recap.

### Scripture Anchors (Reference → Purpose)
Bullet list.
Format:
Book Chapter:Verse — explanation of how it was used in the argument.
Include chapter and verse when explicitly cited.

### Application (Concrete Imperatives)
3–7 bullet points.
Use plain language.
What does the preacher tell the congregation to do, trust, believe,
repent of, pursue, endure, or rest in?

### Paraphrased Key Claims
3–7 short doctrinal statements expressed clearly.
Do not quote verbatim. Paraphrase faithfully.
Include at least one claim that reflects the grace/effort balance
of the sermon if that tension is present at all.
Include at least one claim that uses the preacher's own language
or close paraphrase of how they expressed grace, assurance,
or hope — not a generic theological statement.

LENGTH GUIDANCE:
Target 500–800 words for a 30–45 minute sermon.
Shorter sermons may scale proportionally.
Coverage is more important than brevity. For sermons with complex
multi-part arguments, named theological sources, or substantial
application sections, extend to 900 words rather than compress coverage.

INTERNAL QUALITY CHECK (do not output):
- Did I explicitly capture hope if present?
- Did I explicitly capture grace language if present?
- Did I preserve emotional tone, not just argument structure?
- Did I include scripture purpose, not just references?
- Did I clearly preserve the preacher's burden?
- Did I distinguish what the text says from how the preacher delivered it?
- Did I capture grace language even if it appeared briefly?
- Did I preserve the closing movement with proportional weight?
- Did I use the preacher's actual language or close paraphrase
  for at least one grace/hope/assurance claim?
- If this is an expository sermon, did I capture the preacher's
  application language separately from the text's content?

Only output the structured summary.
"""


# ---------------------------
# OpenAI Client
# ---------------------------

def _get_openai_client() -> OpenAI:
    """Get OpenAI client with API key from env or .env file."""
    api_key = os.environ.get("OPENAI_API_KEY")

    if not api_key:
        env_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
        if os.path.exists(env_file):
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("OPENAI_API_KEY="):
                        api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
                        break

    if not api_key:
        raise ValueError(
            "OpenAI API key not found. Please either:\n"
            "  1. Set OPENAI_API_KEY environment variable, or\n"
            "  2. Create a .env file with: OPENAI_API_KEY=your-key-here"
        )

    return OpenAI(api_key=api_key)


def _generate_summary_v2(transcript: str, model: str = "gpt-4o") -> str:
    """
    Generate summary using Summary Generator V2 prompt.
    Returns the generated summary text.
    """
    client = _get_openai_client()

    user_prompt = SUMMARY_GENERATOR_V2_USER.format(transcript=transcript)

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SUMMARY_GENERATOR_V2_SYSTEM},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0.3,
        max_tokens=2000
    )

    summary = response.choices[0].message.content or ""
    return summary.strip()


# ---------------------------
# Database operations
# ---------------------------

def _connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=5000;")
    return con


def _get_transcripts(con: sqlite3.Connection, video_ids: List[str]) -> Dict[str, str]:
    """
    Retrieve full_text for specified video_ids.
    Returns dict mapping video_id -> full_text.
    """
    placeholders = ",".join("?" * len(video_ids))
    query = f"""
        SELECT video_id, full_text
        FROM transcripts
        WHERE video_id IN ({placeholders})
          AND full_text IS NOT NULL
          AND LENGTH(full_text) > 0
    """

    rows = con.execute(query, video_ids).fetchall()

    return {row["video_id"]: row["full_text"] for row in rows}


def _update_summary(con: sqlite3.Connection, video_id: str, new_summary: str) -> None:
    """
    Update summary_text for a video_id.
    """
    con.execute(
        "UPDATE transcripts SET summary_text = ? WHERE video_id = ?",
        (new_summary, video_id)
    )
    con.commit()


def _get_all_video_ids(con: sqlite3.Connection) -> List[str]:
    """
    Get all video_ids from transcripts where full_text is not null
    and word count >= 100.
    """
    query = """
        SELECT video_id
        FROM transcripts
        WHERE full_text IS NOT NULL
          AND LENGTH(full_text) > 0
          AND LENGTH(full_text) - LENGTH(REPLACE(full_text, ' ', '')) + 1 >= 100
        ORDER BY video_id
    """
    rows = con.execute(query).fetchall()
    return [row["video_id"] for row in rows]


# ---------------------------
# Main regeneration
# ---------------------------

def regenerate_summaries(db_path: str, video_ids: List[str]) -> Dict[str, Any]:
    """
    Regenerate summaries for specified videos and update database.
    Returns summary of what was done.
    """
    con = _connect(db_path)

    try:
        # Get transcripts
        print(f"Retrieving transcripts for {len(video_ids)} videos...")
        transcripts = _get_transcripts(con, video_ids)

        if len(transcripts) != len(video_ids):
            missing = set(video_ids) - set(transcripts.keys())
            print(f"Warning: {len(missing)} videos not found or missing transcript: {missing}")

        # Generate new summaries
        results = []
        for vid in video_ids:
            if vid not in transcripts:
                print(f"Skipping {vid} - no transcript found")
                continue

            print(f"\nGenerating summary for {vid}...")
            full_text = transcripts[vid]
            word_count = len(full_text.split())
            print(f"  Transcript length: {word_count} words")

            try:
                new_summary = _generate_summary_v2(full_text)
                summary_words = len(new_summary.split())
                print(f"  Generated summary: {summary_words} words")

                # Update database
                _update_summary(con, vid, new_summary)
                print(f"  ✓ Updated database")

                results.append({
                    "video_id": vid,
                    "transcript_words": word_count,
                    "summary_words": summary_words,
                    "status": "success"
                })

            except Exception as e:
                print(f"  ✗ Error: {e}")
                results.append({
                    "video_id": vid,
                    "status": "error",
                    "error": str(e)
                })

        return {
            "total_requested": len(video_ids),
            "total_processed": len([r for r in results if r["status"] == "success"]),
            "results": results
        }

    finally:
        con.close()


# ---------------------------
# CLI
# ---------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Regenerate summaries using Summary Generator V2 prompt"
    )
    parser.add_argument(
        "--db",
        default="db/digital_pulpit.db",
        help="Path to database file"
    )
    parser.add_argument(
        "--video_ids",
        nargs="+",
        help="Video IDs to regenerate (space-separated)"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Regenerate ALL summaries (overrides --video_ids)"
    )

    args = parser.parse_args()

    # Determine which video_ids to process
    if args.all:
        print("Fetching all video_ids from database...")
        con = _connect(args.db)
        try:
            video_ids = _get_all_video_ids(con)
            print(f"Found {len(video_ids)} videos with transcripts (word_count >= 100)")
        finally:
            con.close()
    elif args.video_ids:
        video_ids = args.video_ids
    else:
        # Default to the 5 experiment videos
        video_ids = [
            "76d5c3d18fa8cd0b",  # Cana
            "588ea171ce4164a1",  # Tracking in the Dirt
            "3b870a7927f246d4",  # Obedience Brings Blessing
            "78db72267e74fa70",  # 1 Timothy
            "28813a80edb48873",  # Take Heart
        ]

    print(f"Regenerating summaries for {len(video_ids)} videos using Summary Generator V2.1...")
    print("=" * 60)

    result = regenerate_summaries(args.db, video_ids)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(json.dumps(result, indent=2))

    if result["total_processed"] < result["total_requested"]:
        print(f"\n⚠ Warning: Only {result['total_processed']}/{result['total_requested']} summaries were successfully generated")
        sys.exit(1)


if __name__ == "__main__":
    main()
