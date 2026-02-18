import os
import csv
import logging
from dotenv import load_dotenv

# ✅ LOAD ENV FILE
load_dotenv()

logger = logging.getLogger(__name__)

DATABASE_PATH = os.getenv("DATABASE_PATH", "db/digital_pulpit.db")

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
DASHBOARD_PASSWORD = os.getenv("DASHBOARD_PASSWORD")

MAX_MINUTES_PER_RUN = int(os.getenv("MAX_MINUTES_PER_RUN", "180"))
MAX_VIDEOS_PER_RUN = int(os.getenv("MAX_VIDEOS_PER_RUN", "25"))
CAPTIONS_ONLY = os.getenv("CAPTIONS_ONLY", "0") == "1"


def load_channels_csv(path="data/channels.csv"):
    """Load channel rows from CSV/TSV."""

    channels = []

    if not os.path.exists(path):
        logger.warning(f"channels.csv not found at {path}")
        return channels

    with open(path, "r", encoding="utf-8", newline="") as f:
        sample = f.read(4096)
        f.seek(0)

        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=[",", "\t"])
        except Exception:
            dialect = csv.excel

        reader = csv.DictReader(f, dialect=dialect)

        for row in reader:
            name = (row.get("channel_name") or "").strip()
            url = (row.get("channel_url") or "").strip()
            channel_id = (row.get("channel_id") or "").strip()

            if name and url:
                channels.append({
                    "name": name,
                    "url": url,
                    "channel_id": channel_id or None,
                })

    return channels


def load_theology_config(path="data/digital_pulpit_config.json"):
    """Load theology configuration from JSON file."""
    import json

    if not os.path.exists(path):
        logger.warning(f"Theology config not found at {path}")
        return None

    try:
        with open(path, "r", encoding="utf-8") as f:
            config = json.load(f)
        return config
    except Exception as e:
        logger.error(f"Failed to load theology config: {e}")
        return None
