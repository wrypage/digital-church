#!/usr/bin/env python3
"""
run_summary_v2_experiment.py

Orchestrator script that:
1. Regenerates summaries using Summary Generator V2
2. Recomputes brain results for those videos
3. Re-runs brain experiment to compare results

Usage:
  python run_summary_v2_experiment.py --db db/digital_pulpit.db
"""

import argparse
import json
import subprocess
import sys
from typing import Dict, Any


VIDEO_IDS = [
    "76d5c3d18fa8cd0b",  # Cana/Selah - Ground Truth
    "588ea171ce4164a1",  # Tracking in the Dirt - Grace-Heavy
    "3b870a7927f246d4",  # Obedience Brings Blessing - Effort-Heavy
    "78db72267e74fa70",  # 1 Timothy - Doctrinal
    "28813a80edb48873",  # Take Heart - Highest Theo Density
]


def run_command(cmd: list, description: str) -> bool:
    """Run a command and return success status."""
    print(f"\n{'=' * 60}")
    print(f"STEP: {description}")
    print(f"{'=' * 60}")
    print(f"Running: {' '.join(cmd)}\n")

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


def main():
    parser = argparse.ArgumentParser(
        description="Run complete Summary V2 experiment workflow"
    )
    parser.add_argument(
        "--db",
        default="db/digital_pulpit.db",
        help="Path to database file"
    )

    args = parser.parse_args()

    print("=" * 60)
    print("SUMMARY GENERATOR V2 EXPERIMENT WORKFLOW")
    print("=" * 60)
    print(f"\nDatabase: {args.db}")
    print(f"Video IDs: {len(VIDEO_IDS)}")
    for vid in VIDEO_IDS:
        print(f"  - {vid}")
    print()

    # Step 1: Regenerate summaries
    success = run_command(
        ["python", "-m", "engine.regenerate_summaries_v2", "--db", args.db],
        "Step 1: Regenerate summaries with V2 prompt"
    )
    if not success:
        print("\n✗ Workflow failed at Step 1")
        sys.exit(1)

    # Step 2: Recompute brain results
    video_id_args = []
    for vid in VIDEO_IDS:
        video_id_args.extend(["--video_id", vid])

    success = run_command(
        ["python", "-m", "engine.brain", "--recompute"] + video_id_args,
        "Step 2: Recompute brain results for updated summaries"
    )
    if not success:
        print("\n✗ Workflow failed at Step 2")
        sys.exit(1)

    # Step 3: Re-run brain experiment
    success = run_command(
        ["python", "-m", "engine.brain_experiment", "--db", args.db],
        "Step 3: Re-run brain variant experiment"
    )
    if not success:
        print("\n✗ Workflow failed at Step 3")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("✓ WORKFLOW COMPLETED SUCCESSFULLY")
    print("=" * 60)
    print("\nCheck out/experiments/ for the new brain_variants_*.md file")
    print("Compare with previous results to see if axis flips were resolved.")


if __name__ == "__main__":
    main()
