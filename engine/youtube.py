import os
import re
import logging
import time
import requests
import subprocess
from datetime import datetime, timedelta, timezone
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from engine.config import YOUTUBE_API_KEY
from engine import db

logger = logging.getLogger("digital_pulpit")


def get_youtube_service():
    if not YOUTUBE_API_KEY:
        raise ValueError("YOUTUBE_API_KEY not set")
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY)


def _api_call_with_backoff(fn, max_retries=4):
    for attempt in range(max_retries):
        try:
            return fn()
        except HttpError as e:
            if e.resp.status in (429, 403):
                wait = 2 ** (attempt + 1)
                logger.warning(f"YouTube API {e.resp.status}, backing off {wait}s (attempt {attempt+1}/{max_retries})")
                time.sleep(wait)
                continue
            raise
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "quotaExceeded" in err_str:
                wait = 2 ** (attempt + 1)
                logger.warning(f"YouTube API rate limit, backing off {wait}s (attempt {attempt+1}/{max_retries})")
                time.sleep(wait)
                continue
            raise
    raise Exception(f"YouTube API call failed after {max_retries} retries (rate limited)")


def _clean(s):
    return (s or "").strip()


def _clean_handle(handle: str) -> str:
    h = _clean(handle)
    if h.startswith("@"):
        h = h[1:]
    return h


def _resolve_with_ytdlp(url: str):
    """
    Most reliable resolver in hosted environments:
    yt-dlp can resolve @handles and return channel_id (UC...)
    """
    if not url:
        return None
    try:
        cookies_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "cookies.txt")
        cmd = [
            "yt-dlp", "--print", "channel_id", "--no-warnings",
            "--no-playlist",
        ]
        if os.path.exists(cookies_path) and os.path.getsize(cookies_path) > 0:
            cmd.extend(["--cookies", cookies_path])
        cmd.append(url)
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if r.returncode != 0:
            return None
        cid = (r.stdout or "").strip().splitlines()
        cid = cid[0].strip() if cid else ""
        if cid.startswith("UC"):
            return cid
        return None
    except Exception:
        return None


def _search_channel_id(query: str):
    q = _clean(query)
    if not q:
        return None
    yt = get_youtube_service()
    try:
        resp = _api_call_with_backoff(
            lambda: yt.search().list(
                part="id,snippet",
                q=q,
                type="channel",
                maxResults=1,
            ).execute()
        )
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

    if not url and handle_field:
        url = f"https://www.youtube.com/@{handle_field}"

    # 1) direct /channel/UC...
    channel_id_match = re.search(r"/channel/(UC[\w-]+)", url)
    if channel_id_match:
        cid = channel_id_match.group(1)
        logger.info(f"Extracted channel ID from URL for {name}: {cid}")
        return cid, "url_extract"

    # 2) yt-dlp resolver (very reliable for @handles)
    if url:
        cid = _resolve_with_ytdlp(url)
        if cid:
            logger.info(f"yt-dlp resolved {name} to {cid}")
            return cid, "ytdlp"

    # 3) Try YouTube API forHandle (sometimes works)
    yt = get_youtube_service()
    handle = handle_field
    if not handle:
        m = re.search(r"/@([\w.-]+)", url)
        if m:
            handle = _clean_handle(m.group(1))

    if handle:
        try:
            resp = _api_call_with_backoff(
                lambda: yt.channels().list(part="id,snippet",
                                           forHandle=handle).execute()
            )
            if resp.get("items"):
                cid = resp["items"][0]["id"]
                logger.info(f"Resolved @{handle} to {cid}")
                return cid, "handle"
        except Exception as e:
            logger.warning(f"Handle resolution failed for @{handle}: {e}")

    # 4) HTML fallback
    if url:
        try:
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

    # 5) Search fallback
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


def discover_videos(channel_id, max_results=25):
    yt = get_youtube_service()
    fourteen_days_ago = (datetime.now(timezone.utc) -
                         timedelta(days=14)).isoformat()

    try:
        search_resp = _api_call_with_backoff(
            lambda: yt.search().list(
                part="id,snippet",
                channelId=channel_id,
                type="video",
                order="date",
                publishedAfter=fourteen_days_ago,
                maxResults=max_results,
            ).execute()
        )
    except Exception as e:
        logger.error(f"Search failed for channel {channel_id}: {e}")
        return []

    video_ids = [
        item["id"]["videoId"] for item in search_resp.get("items", [])
        if item.get("id", {}).get("videoId")
    ]
    if not video_ids:
        logger.info(f"No recent videos for channel {channel_id}")
        return []

    details_resp = _api_call_with_backoff(
        lambda: yt.videos().list(
            part="contentDetails,snippet",
            id=",".join(video_ids),
        ).execute()
    )

    candidates = []
    for item in details_resp.get("items", []):
        duration = parse_duration(item["contentDetails"]["duration"])
        if duration < 62:
            continue
        candidates.append({
            "video_id": item["id"],
            "title": item["snippet"]["title"],
            "published_at": item["snippet"]["publishedAt"],
            "duration_seconds": duration,
            "channel_id": channel_id,
        })

    candidates.sort(key=lambda x: x["published_at"], reverse=True)

    for v in candidates:
        db.upsert_video(v["video_id"], v["channel_id"], v["title"],
                        v["published_at"], v["duration_seconds"], "discovered")

    logger.info(
        f"Discovered {len(candidates)} candidate videos for channel {channel_id}"
    )
    return candidates
