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
    """Load channel rows from CSV/TSV.

    Supports two schemas:
    1) channel_name, channel_url, channel_id   (original)
    2) Channel Name, YouTube Handle, Channel ID (and other extra columns)
       - If only a handle is provided (e.g., @TDJakesOfficial), we construct the channel URL.
    """
    channels = []
    if not os.path.exists(path):
        logger.warning(f"channels.csv not found at {path}")
        return channels

    # Auto-detect comma vs tab-delimited files (common when pasting from spreadsheets)
    with open(path, "r", encoding="utf-8", newline="") as f:
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=[",", "\t"])
        except Exception:
            dialect = csv.excel

        reader = csv.DictReader(f, dialect=dialect)

        for row in reader:
            # Schema A
            name = (row.get("channel_name") or "").strip()
            url = (row.get("channel_url") or "").strip()
            channel_id = (row.get("channel_id") or "").strip()

            # Schema B (your pastor list)
            if not (name or url or channel_id):
                name = (row.get("Channel Name") or "").strip()
                handle = (row.get("YouTube Handle") or "").strip()
                channel_id = (row.get("Channel ID") or "").strip()

                if handle:
                    handle_clean = handle.strip()
                    if handle_clean.startswith("@"):
                        handle_clean = handle_clean[1:]
                    url = f"https://www.youtube.com/@{handle_clean}"

            channels.append({
                "name": name,
                "url": url,
                "channel_id": channel_id,
            })

    return channels


def load_theology_config(path="data/digital_pulpit_config.json"):
    if not os.path.exists(path):
        logger.warning(f"Config not found at {path}")
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
