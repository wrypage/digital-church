#!/usr/bin/env python3
"""
Extract channel IDs from channels.csv URLs using yt-dlp (no API quota needed)
"""
import csv
import subprocess
import os

def get_channel_id_ytdlp(url):
    """Use yt-dlp to extract channel ID without using YouTube API"""
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

    if not os.path.exists(input_file):
        print(f"Error: {input_file} not found")
        return

    channels = []
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            channels.append(row)

    print(f"Processing {len(channels)} channels...")
    updated = 0

    for i, channel in enumerate(channels, 1):
        url = channel.get('channel_url', '').strip()
        existing_id = channel.get('channel_id', '').strip()
        name = channel.get('channel_name', 'Unknown')

        print(f"\n[{i}/{len(channels)}] {name}")

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

    print(f"\n✓ Updated {updated} channels")
    print(f"✓ Saved to: {output_file}")
    print(f"\nNext step: Replace {input_file} with {output_file}")

if __name__ == "__main__":
    main()
