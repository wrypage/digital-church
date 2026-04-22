# script_generator.py
# The Council of 139 — Script Generator (v1.0)
#
# Purpose:
# - Takes a weekly heartbeat_report (from analysis_engine output)
# - Identifies the "Drift of the Week"
# - Selects the best quotes for each character based on affinities + scoring
# - Generates a short cinematic dialogue arc (60–90 seconds) that is data-grounded
#
# Expected heartbeat_report shape (defensive; fields are optional):
# {
#   "week_of": "2026-02-09",
#   "drift_metrics": {
#       "Hope_vs_Fear": {"z_score": 2.41, "percent_change": 33, "current": 1.32, "baseline": 0.99},
#       ...
#   },
#   "top_quotes": [
#       {
#         "text": "....",
#         "layers": ["L1_Soteriology", "L9_Anthropology"],
#         "tags": ["Grace", "Repentance"],
#         "tone": "Warning",
#         "base_quote_score": 84.2,
#         "character_scores": {"Sully": 92.1, "Elena": 70.4},
#         "source": {"video_id": "...", "channel_name": "...", "start": 123.4, "end": 140.1}
#       }
#   ],
#   "tone_summary": {"Hope": 22, "Fear (Tone)": 31, "Lament": 12, ...},
#   "density_summary": {"D_theta_mean": 18.4, "D_theta_change_pct": 7.2}
# }

import json
import random
from typing import Any, Dict, List, Optional, Tuple


class AssemblyScriptDirector:
    def __init__(
        self,
        config_path: str = "digital_pulpit_config.json",
        seed: Optional[int] = None,
        top_k_pool: int = 5,
    ):
        with open(config_path, "r") as f:
            self.config = json.load(f)

        # Character affinities should be a dict: { "Sully": [ ... ], "Elena": [ ... ] ... }
        # We'll keep it defensive.
        self.avatars: Dict[str, List[str]] = self.config.get("character_affinities", {})

        # If you want reproducible scripts in tests
        if seed is not None:
            random.seed(seed)

        self.top_k_pool = max(1, int(top_k_pool))

        # Optional: drift template routing based on metric name patterns
        self.template_map = [
            (["Hope_vs_Fear", "Fear", "Anxiety"], "AFFECT_LEADS"),
            (["Doctrine_vs_Experience", "Repentance_vs_Breakthrough", "Sin_Density"], "DOCTRINE_LEADS"),
            (["Scripture_vs_Story"], "SCRIPTURE_LEADS"),
            (["Global"], "GLOBAL_LEADS"),
        ]

    # ----------------------------
    # Drift Identification
    # ----------------------------

    def identify_primary_drift(self, heartbeat_report: Dict[str, Any]) -> Tuple[str, str, Optional[Dict[str, Any]]]:
        """
        Returns: (metric_name, direction, metric_payload)
        direction ∈ {"SURGE","DECLINE","STABLE"}
        """
        metrics = heartbeat_report.get("drift_metrics") or {}
        if not metrics:
            return "Stable_Growth", "STABLE", None

        # Choose metric with largest absolute z-score, but be defensiv
