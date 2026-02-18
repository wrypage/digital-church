#!/usr/bin/env python3
"""
Extract channel IDs slowly with delays to avoid rate limits
"""
import csv
import subprocess
import os
import time

def get_channel_id_ytdlp(url):
    """Use yt-dlp to extract channel ID"""
    try:
        cookies_path = "cookies.txt" if os.path.exists("cookies.txt") else None
        cmd = ["yt-dlp", "--print", "channel_id", "--no-warnings", "--no-playlist"]
        if cookies_path:
            cmd.extend(["--cookies", cookies_path])
        cmd.append(url)

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            channel_id = result.stdout.strip().split('\n')[0].strip()
            if channel_id.startswith("UC"):
                return channel_id
    except Exception as e:
        print(f"  Error: {e}")
    return None

def main():
    input_file = "data/channels.csv"
    output_file = "data/channels_with_ids.csv"

    # Read existing output if it exists
    existing_ids = {}
    if os.path.exists(output_file):
        with open(output_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get('channel_url', '').strip()
                cid = row.get('channel_id', '').strip()
                if url and cid:
                    existing_ids[url] = cid
        print(f"Loaded {len(existing_ids)} existing IDs")

    channels = []
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            channels.append(row)

    print(f"Processing {len(channels)} channels (with 5s delay between requests)...")
    updated = 0

    for i, channel in enumerate(channels, 1):
        url = channel.get('channel_url', '').strip()
        existing_id = channel.get('channel_id', '').strip()
        name = channel.get('channel_name', 'Unknown')

        print(f"\n[{i}/{len(channels)}] {name}")

        # Check if we already have it
        if url in existing_ids:
            channel['channel_id'] = existing_ids[url]
            print(f"  ✓ Already extracted: {existing_ids[url]}")
            continue

        if existing_id and existing_id.startswith('UC'):
            print(f"  ✓ Already has ID: {existing_id}")
            continue

        if not url:
            print(f"  ✗ No URL")
            continue

        print(f"  Fetching ID from: {url}")
        channel_id = get_channel_id_ytdlp(url)

        if channel_id:
            channel['channel_id'] = channel_id
            updated += 1
            print(f"  ✓ Got ID: {channel_id}")
        else:
            print(f"  ✗ Failed to get ID")

        # Wait 5 seconds between requests to avoid rate limits
        if i < len(channels):
            print("  ⏳ Waiting 5s...")
            time.sleep(5)

    # Write updated CSV
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        fieldnames = ['channel_name', 'channel_url', 'channel_id']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for channel in channels:
            writer.writerow({
                'channel_name': channel.get('channel_name', ''),
                'channel_url': channel.get('channel_url', ''),
                'channel_id': channel.get('channel_id', ''),
            })

    print(f"\n✓ Updated {updated} new channels")
    print(f"✓ Total with IDs: {len([c for c in channels if c.get('channel_id', '').startswith('UC')])}")
    print(f"✓ Saved to: {output_file}")

if __name__ == "__main__":
    main()
