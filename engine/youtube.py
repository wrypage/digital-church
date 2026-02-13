import re
import logging
import requests
from datetime import datetime, timedelta, timezone
from googleapiclient.discovery import build
from engine.config import YOUTUBE_API_KEY
from engine import db

logger = logging.getLogger("digital_pulpit")


def get_youtube_service():
    if not YOUTUBE_API_KEY:
        raise ValueError("YOUTUBE_API_KEY not set")
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY)


def _clean(s):
    return (s or "").strip()


def _clean_handle(handle: str) -> str:
    h = _clean(handle)
    if h.startswith("@"):
        h = h[1:]
    return h


def _search_channel_id(query: str):
    """
    Fallback: search for channel by name/handle-like query.
    Returns channelId or None.
    """
    q = _clean(query)
    if not q:
        return None
    yt = get_youtube_service()
    try:
        resp = yt.search().list(
            part="id,snippet",
            q=q,
            type="channel",
            maxResults=1,
        ).execute()
        items = resp.get("items") or []
        if not items:
            return None
        return items[0]["id"].get("channelId")
    except Exception as e:
        logger.warning(f"Channel search fallback failed for '{q}': {e}")
        return None


def resolve_channel_id(channel_info):
    url = _clean(channel_info.get("url"))
    provided_id = _clean(channel_info.get("channel_id"))
    name = _clean(channel_info.get("name"))
    handle_field = _clean_handle(
        channel_info.get("handle") or channel_info.get("youtube_handle")
        or channel_info.get("YouTube Handle") or "")

    if provided_id:
        logger.info(f"Channel ID provided for {name}: {provided_id}")
        return provided_id, "provided"

    # If URL missing but handle present, construct URL
    if not url and handle_field:
        url = f"https://www.youtube.com/@{handle_field}"

    # Direct /channel/UC... extraction
    channel_id_match = re.search(r"/channel/(UC[\w-]+)", url)
    if channel_id_match:
        cid = channel_id_match.group(1)
        logger.info(f"Extracted channel ID from URL for {name}: {cid}")
        return cid, "url_extract"

    yt = get_youtube_service()

    # Resolve by handle (prefer explicit handle, else parse from URL)
    handle = handle_field
    if not handle:
        handle_match = re.search(r"/@([\w.-]+)", url)
        if handle_match:
            handle = _clean_handle(handle_match.group(1))

    if handle:
        try:
            resp = yt.channels().list(part="id,snippet",
                                      forHandle=handle).execute()
            if resp.get("items"):
                cid = resp["items"][0]["id"]
                logger.info(f"Resolved @{handle} to {cid}")
                return cid, "handle"
        except Exception as e:
            logger.warning(f"Handle resolution failed for @{handle}: {e}")

    # Legacy /user/username
    user_match = re.search(r"/user/([\w.-]+)", url)
    if user_match:
        username = user_match.group(1)
        try:
            resp = yt.channels().list(part="id,snippet",
                                      forUsername=username).execute()
            if resp.get("items"):
                cid = resp["items"][0]["id"]
                logger.info(f"Resolved /user/{username} to {cid}")
                return cid, "username"
        except Exception as e:
            logger.warning(f"Username resolution failed for {username}: {e}")

    # HTML fallback (sometimes works even when forHandle fails)
    if url:
        try:
            logger.info(f"HTML fallback fetch for {url}")
            resp = requests.get(url,
                                timeout=15,
                                headers={"User-Agent": "Mozilla/5.0"})
            match = re.search(r'"channelId"\s*:\s*"(UC[\w-]+)"', resp.text)
            if match:
                cid = match.group(1)
                logger.info(f"HTML fallback resolved {name} to {cid}")
                return cid, "html_fallback"
        except Exception as e:
            logger.warning(f"HTML fallback failed for {name}: {e}")

    # FINAL fallback: YouTube search by handle or name
    # Try handle first (more precise), then name
    if handle_field:
        cid = _search_channel_id(f"@{handle_field}")
        if cid:
            logger.info(f"Search fallback resolved @{handle_field} to {cid}")
            return cid, "search_handle"

    cid = _search_channel_id(name)
    if cid:
        logger.info(f"Search fallback resolved '{name}' to {cid}")
        return cid, "search_name"

    logger.error(f"Could not resolve channel ID for {name} ({url})")
    return None, "failed"


def parse_duration(duration_str):
    match = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration_str
                     or "")
    if not match:
        return 0
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds


def discover_videos(channel_id, max_results=10):
    yt = get_youtube_service()
    seven_days_ago = (datetime.now(timezone.utc) -
                      timedelta(days=7)).isoformat()

    try:
        search_resp = yt.search().list(
            part="id,snippet",
            channelId=channel_id,
            type="video",
            eventType="completed",
            order="date",
            publishedAfter=seven_days_ago,
            maxResults=max_results,
        ).execute()
    except Exception as e:
        logger.error(f"Search failed for channel {channel_id}: {e}")
        return []

    video_ids = [
        item["id"]["videoId"] for item in search_resp.get("items", [])
    ]
    if not video_ids:
        logger.info(f"No recent videos for channel {channel_id}")
        return []

    details_resp = yt.videos().list(
        part="contentDetails,snippet",
        id=",".join(video_ids),
    ).execute()

    qualifying = []
    for item in details_resp.get("items", []):
        duration = parse_duration(item["contentDetails"]["duration"])
        if duration < 62:
            continue
        qualifying.append({
            "video_id": item["id"],
            "title": item["snippet"]["title"],
            "published_at": item["snippet"]["publishedAt"],
            "duration_seconds": duration,
            "channel_id": channel_id,
        })

    qualifying.sort(key=lambda x: x["published_at"], reverse=True)
    selected = qualifying[:3]

    for v in selected:
        db.upsert_video(v["video_id"], v["channel_id"], v["title"],
                        v["published_at"], v["duration_seconds"], "discovered")

    logger.info(f"Discovered {len(selected)} videos for channel {channel_id}")
    return selected
