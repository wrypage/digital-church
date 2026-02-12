import os
import json
import csv
import logging

logger = logging.getLogger("digital_pulpit")

DATABASE_PATH = os.environ.get("DATABASE_PATH", "db/digital_pulpit.db")
MAX_MINUTES_PER_RUN = int(os.environ.get("MAX_MINUTES_PER_RUN", "180"))
MAX_VIDEOS_PER_RUN = int(os.environ.get("MAX_VIDEOS_PER_RUN", "120"))
KEEP_AUDIO_ON_FAIL = os.environ.get("KEEP_AUDIO_ON_FAIL", "false").lower() == "true"
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "")
TMP_AUDIO_DIR = "tmp_audio"


def load_channels_csv(path="data/channels.csv"):
    channels = []
    if not os.path.exists(path):
        logger.warning(f"channels.csv not found at {path}")
        return channels
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            channels.append({
                "name": row.get("channel_name", "").strip(),
                "url": row.get("channel_url", "").strip(),
                "channel_id": row.get("channel_id", "").strip(),
            })
    return channels


def load_theology_config(path="data/digital_pulpit_config.json"):
    if not os.path.exists(path):
        logger.warning(f"Config not found at {path}")
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
