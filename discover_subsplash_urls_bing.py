#!/usr/bin/env python3
"""
Discover Subsplash RSS feed URLs using Bing Web Search API.
Collects up to 100 valid RSS feed URLs and writes them to feeds.txt.
"""

import os
import re
import sys
import time
import requests

# Configuration
BING_API_ENDPOINT = "https://api.bing.microsoft.com/v7.0/search"
RSS_URL_PATTERN = re.compile(r'^https://podcasts\.subsplash\.com/[A-Za-z0-9]+/podcast\.rss$')
OUTPUT_FILE = "feeds.txt"

# Queries to try (fallback if first returns no results)
QUERIES = [
    "site:podcasts.subsplash.com inurl:podcast.rss",
    'site:podcasts.subsplash.com "podcast.rss"'
]


def search_bing(api_key, query, count=50, offset=0):
    """
    Execute a Bing Web Search API request.

    Args:
        api_key: Bing Search API key
        query: Search query string
        count: Number of results to return (max 50)
        offset: Pagination offset

    Returns:
        List of URLs from the search results, or None if error
    """
    headers = {
        "Ocp-Apim-Subscription-Key": api_key
    }

    params = {
        "q": query,
        "count": count,
        "offset": offset
    }

    try:
        response = requests.get(BING_API_ENDPOINT, headers=headers, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()

        # Extract URLs from webPages.value
        urls = []
        if "webPages" in data and "value" in data["webPages"]:
            for result in data["webPages"]["value"]:
                if "url" in result:
                    urls.append(result["url"])

        return urls

    except requests.exceptions.RequestException as e:
        print(f"Error searching Bing (offset={offset}): {e}", file=sys.stderr)
        return None


def filter_valid_rss_urls(urls):
    """
    Filter URLs to only include valid Subsplash podcast RSS feeds.

    Args:
        urls: List of URLs to filter

    Returns:
        Set of valid RSS feed URLs
    """
    valid_urls = set()

    for url in urls:
        if RSS_URL_PATTERN.match(url):
            valid_urls.add(url)

    return valid_urls


def discover_feeds(api_key):
    """
    Discover Subsplash RSS feeds using Bing Search API.

    Args:
        api_key: Bing Search API key

    Returns:
        Sorted list of unique RSS feed URLs
    """
    all_valid_feeds = set()
    total_results = 0

    # Try each query
    for query_idx, query in enumerate(QUERIES):
        print(f"Searching with query: {query}")
        query_feeds = set()

        # Search two pages (offset 0 and 50)
        for offset in [0, 50]:
            print(f"  Fetching results (offset={offset})...")

            urls = search_bing(api_key, query, count=50, offset=offset)

            if urls is None:
                print(f"  Failed to fetch results at offset {offset}")
                continue

            total_results += len(urls)
            print(f"  Received {len(urls)} results")

            # Filter for valid RSS URLs
            valid_feeds = filter_valid_rss_urls(urls)
            query_feeds.update(valid_feeds)
            print(f"  Found {len(valid_feeds)} valid RSS feeds in this batch")

            # Sleep between requests
            if offset == 0:  # Sleep before next page
                time.sleep(0.5)

        print(f"  Total valid feeds from this query: {len(query_feeds)}")
        all_valid_feeds.update(query_feeds)

        # If we got results from first query, no need to try fallback
        if query_idx == 0 and len(query_feeds) > 0:
            print(f"First query returned results, skipping fallback query")
            break

        # Sleep between queries
        if query_idx == 0 and len(QUERIES) > 1:
            time.sleep(0.5)

    print(f"\nTotal Bing results returned: {total_results}")
    print(f"Total unique valid RSS feeds found: {len(all_valid_feeds)}")

    # Sort alphabetically
    return sorted(all_valid_feeds)


def write_feeds(feeds, output_file):
    """
    Write feed URLs to output file (one per line).

    Args:
        feeds: List of feed URLs
        output_file: Path to output file
    """
    with open(output_file, 'w', encoding='utf-8') as f:
        for feed in feeds:
            f.write(feed + '\n')

    print(f"Wrote {len(feeds)} feeds to {output_file}")


def main():
    """Main entry point."""
    # Check for API key
    api_key = os.environ.get('BING_SEARCH_API_KEY')

    if not api_key:
        print("ERROR: BING_SEARCH_API_KEY environment variable not set", file=sys.stderr)
        print("Please set it with:", file=sys.stderr)
        print("  export BING_SEARCH_API_KEY='your-api-key-here'", file=sys.stderr)
        sys.exit(1)

    print("=" * 60)
    print("Subsplash RSS Feed Discovery (Bing Web Search API)")
    print("=" * 60)
    print()

    # Discover feeds
    feeds = discover_feeds(api_key)

    # Write to file
    if feeds:
        write_feeds(feeds, OUTPUT_FILE)
        print()
        print("✓ Discovery complete!")
    else:
        print()
        print("No valid RSS feeds found.")
        sys.exit(1)


if __name__ == "__main__":
    main()
