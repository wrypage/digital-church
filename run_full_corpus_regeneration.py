#!/usr/bin/env python3
"""
run_full_corpus_regeneration.py

Full corpus regeneration workflow:
1. Regenerate all summaries with Summary Generator v2.1
2. Recompute Brain for all sermons (new summaries + config v5.3)
3. Re-run 5-sermon experiment
4. Report axis flip statistics

Usage:
  python run_full_corpus_regeneration.py --db db/digital_pulpit.db --max_cost_usd 10.0
"""

import argparse
import json
import subprocess
import sys
import sqlite3
from typing import Dict, Any


def run_command(cmd: list, description: str) -> bool:
    """Run a command and return success status."""
    print(f"\n{'=' * 70}")
    print(f"STEP: {description}")
    print(f"{'=' * 70}")
    print(f"Command: {' '.join(cmd)}\n")

    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=False,
            text=True
        )
        print(f"\n✓ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n✗ {description} failed with exit code {e.returncode}")
        return False


def get_sermon_count(db_path: str) -> int:
    """Count sermons with transcripts."""
    con = sqlite3.connect(db_path)
    count = con.execute("""
        SELECT COUNT(*)
        FROM transcripts t
        JOIN videos v ON t.video_id = v.video_id
        WHERE t.full_text IS NOT NULL
          AND LENGTH(t.full_text) > 0
    """).fetchone()[0]
    con.close()
    return count


def main():
    parser = argparse.ArgumentParser(
        description="Full corpus regeneration with Summary Generator v2.1 + Config v5.3"
    )
    parser.add_argument(
        "--db",
        default="db/digital_pulpit.db",
        help="Path to database file"
    )
    parser.add_argument(
        "--max_cost_usd",
        type=float,
        default=10.0,
        help="Maximum cost in USD for summary regeneration"
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="Show what would be done without doing it"
    )

    args = parser.parse_args()

    print("=" * 70)
    print("FULL CORPUS REGENERATION WORKFLOW")
    print("=" * 70)
    print(f"\nDatabase: {args.db}")
    print(f"Max Cost: ${args.max_cost_usd:.2f}")

    # Count sermons
    sermon_count = get_sermon_count(args.db)
    estimated_cost = sermon_count * 0.015  # ~$0.015 per sermon average

    print(f"\nSermons to process: {sermon_count}")
    print(f"Estimated cost: ${estimated_cost:.2f}")

    if estimated_cost > args.max_cost_usd:
        print(f"\n⚠️  WARNING: Estimated cost (${estimated_cost:.2f}) exceeds max (${args.max_cost_usd:.2f})")
        response = input("Continue anyway? (yes/no): ")
        if response.lower() != "yes":
            print("Aborted.")
            sys.exit(1)

    if args.dry_run:
        print("\n[DRY RUN MODE - No changes will be made]")
        print("\nWould execute:")
        print("1. Regenerate all summaries with v2.1 prompt")
        print("2. Recompute Brain for all sermons")
        print("3. Re-run 5-sermon experiment")
        print("4. Generate axis flip report")
        return

    # Step 1: Regenerate all summaries
    print("\n" + "=" * 70)
    print("PHASE 1: REGENERATE ALL SUMMARIES")
    print("=" * 70)
    print(f"This will take ~{sermon_count * 3}s (~{sermon_count * 3 / 60:.1f} minutes)")
    print("Progress will be shown...")

    success = run_command(
        ["python", "-m", "engine.regenerate_summaries_v2", "--db", args.db],
        "Phase 1: Regenerate summaries with Summary Generator v2.1"
    )
    if not success:
        print("\n✗ Workflow failed at Phase 1")
        sys.exit(1)

    # Step 2: Recompute Brain for all sermons
    print("\n" + "=" * 70)
    print("PHASE 2: RECOMPUTE BRAIN FOR ALL SERMONS")
    print("=" * 70)
    print("This will apply:")
    print("  - New summaries (v2.1 with closing emphasis)")
    print("  - Config v5.3 (expanded hope/grace keywords)")
    print("  - Updated baselines automatically")

    success = run_command(
        ["python", "-m", "engine.brain", "--recompute"],
        "Phase 2: Recompute Brain with new summaries + config v5.3"
    )
    if not success:
        print("\n✗ Workflow failed at Phase 2")
        sys.exit(1)

    # Step 3: Re-run 5-sermon experiment
    print("\n" + "=" * 70)
    print("PHASE 3: RE-RUN 5-SERMON EXPERIMENT")
    print("=" * 70)

    success = run_command(
        ["python", "-m", "engine.brain_experiment", "--db", args.db],
        "Phase 3: Re-run brain variant experiment"
    )
    if not success:
        print("\n✗ Workflow failed at Phase 3")
        sys.exit(1)

    # Step 4: Generate report
    print("\n" + "=" * 70)
    print("PHASE 4: GENERATE AXIS FLIP REPORT")
    print("=" * 70)

    # Count total axis flips across corpus
    con = sqlite3.connect(args.db)

    # This would require storing baseline before regeneration
    # For now, we'll just show the final state

    print("\nWorkflow complete!")
    print("\nNext steps:")
    print("1. Check out/experiments/ for latest brain_variants_*.md")
    print("2. Review axis flip statistics")
    print("3. Validate 1 Timothy hope_vs_fear > 0")
    print("4. Check that no unexpected regressions occurred")

    con.close()


if __name__ == "__main__":
    main()
