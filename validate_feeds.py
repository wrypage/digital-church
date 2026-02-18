#!/usr/bin/env python3
"""
RSS Feed Validation and Analysis Script
Validates Subsplash sermon feeds against quality criteria and generates detailed report.
"""

import re
import time
import feedparser
import requests
from datetime import datetime, timedelta
from collections import defaultdict


# Quality criteria configuration
SERMON_KEYWORDS = [
    'sermon', 'teaching', 'bible study', 'verse-by-verse',
    'expository', 'biblical', 'pastor', 'preaching',
    'calvary chapel', 'church', 'ministry'
]

NON_SERMON_KEYWORDS = [
    'devotional', 'daily devotion', 'announcement',
    'kids', 'children', 'youth group', 'music',
    'news', 'update', 'podcast interview'
]


def parse_duration(duration_str):
    """
    Parse iTunes duration from various formats:
    - Seconds only: "3600"
    - MM:SS format: "60:00"
    - HH:MM:SS format: "1:00:00"

    Returns: duration in seconds
    """
    if not duration_str:
        return None

    try:
        # Try parsing as integer (seconds)
        return int(duration_str)
    except ValueError:
        pass

    # Try parsing as time format
    parts = str(duration_str).split(':')

    try:
        if len(parts) == 2:  # MM:SS
            minutes, seconds = map(int, parts)
            return minutes * 60 + seconds
        elif len(parts) == 3:  # HH:MM:SS
            hours, minutes, seconds = map(int, parts)
            return hours * 3600 + minutes * 60 + seconds
    except (ValueError, AttributeError):
        pass

    return None


def check_active(feed):
    """Check if feed updated within last 7 days"""
    if not feed.entries:
        return False

    try:
        latest_entry = feed.entries[0]
        if hasattr(latest_entry, 'published_parsed') and latest_entry.published_parsed:
            pub_date = datetime(*latest_entry.published_parsed[:6])
        elif hasattr(latest_entry, 'updated_parsed') and latest_entry.updated_parsed:
            pub_date = datetime(*latest_entry.updated_parsed[:6])
        else:
            return "UNKNOWN"

        days_since_update = (datetime.now() - pub_date).days
        return days_since_update <= 7
    except Exception:
        return "UNKNOWN"


def check_duration(feed):
    """Check if average duration is 30-70 minutes"""
    durations = []

    for entry in feed.entries[:5]:
        duration_str = entry.get('itunes_duration')
        if duration_str:
            duration_secs = parse_duration(duration_str)
            if duration_secs:
                durations.append(duration_secs / 60)  # Convert to minutes

    if not durations:
        return "UNKNOWN"

    avg_duration = sum(durations) / len(durations)
    return 30 <= avg_duration <= 70


def check_content_type(feed):
    """Check if feed appears to be sermon-focused"""
    feed_text = ''

    if hasattr(feed.feed, 'title'):
        feed_text += feed.feed.title.lower() + ' '
    if hasattr(feed.feed, 'subtitle'):
        feed_text += feed.feed.subtitle.lower() + ' '
    if hasattr(feed.feed, 'summary'):
        feed_text += feed.feed.summary.lower() + ' '

    has_sermon_indicators = any(keyword in feed_text for keyword in SERMON_KEYWORDS)
    has_non_sermon_indicators = any(keyword in feed_text for keyword in NON_SERMON_KEYWORDS)

    if has_sermon_indicators and not has_non_sermon_indicators:
        return True
    elif has_sermon_indicators:
        return "NEEDS_REVIEW"
    else:
        return False


def check_metadata_quality(feed):
    """Check metadata completeness of first 5 episodes"""
    metadata_scores = []

    for entry in feed.entries[:5]:
        score = 0
        if entry.get('title') and len(entry.title) > 0:
            score += 1
        if entry.get('itunes_duration'):
            score += 1
        if entry.get('published_parsed') or entry.get('updated_parsed'):
            score += 1
        if entry.get('enclosures'):
            score += 1
        if entry.get('summary') or entry.get('description'):
            score += 1

        metadata_scores.append(score / 5)

    if not metadata_scores:
        return "UNKNOWN"

    avg_quality = sum(metadata_scores) / len(metadata_scores)
    return avg_quality >= 0.8


def check_publishing_consistency(feed):
    """Check if publishing schedule is consistent (1-14 days)"""
    pub_dates = []

    for entry in feed.entries[:10]:
        try:
            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                pub_date = datetime(*entry.published_parsed[:6])
            elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                pub_date = datetime(*entry.updated_parsed[:6])
            else:
                continue
            pub_dates.append(pub_date)
        except Exception:
            continue

    if len(pub_dates) < 2:
        return "UNKNOWN"

    intervals = []
    for i in range(len(pub_dates) - 1):
        days_between = (pub_dates[i] - pub_dates[i+1]).days
        if days_between > 0:  # Ignore negative/zero intervals
            intervals.append(days_between)

    if not intervals:
        return "UNKNOWN"

    avg_interval = sum(intervals) / len(intervals)
    return 1 <= avg_interval <= 14


def extract_feed_id(url):
    """Extract feed ID from URL"""
    match = re.search(r'podcasts\.subsplash\.com/([A-Za-z0-9]+)/', url)
    return match.group(1) if match else "UNKNOWN"


def validate_feed(url):
    """Validate a single feed against all criteria"""
    feed_id = extract_feed_id(url)

    print(f"  Validating {feed_id}...")

    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        feed = feedparser.parse(response.content)

        if not feed.entries:
            return {
                'status': 'REJECTED',
                'reason': 'No episodes found',
                'feed_id': feed_id,
                'url': url
            }

        # Run all 5 criteria checks
        criteria = {
            'active': check_active(feed),
            'longform': check_duration(feed),
            'theological': check_content_type(feed),
            'metadata': check_metadata_quality(feed),
            'consistent': check_publishing_consistency(feed)
        }

        # Count criteria passed (True = 1, False/UNKNOWN/NEEDS_REVIEW = 0)
        passed = sum(1 for v in criteria.values() if v is True)

        # Extract metadata
        feed_data = extract_feed_metadata(url, feed, feed_id)
        feed_data['criteria_passed'] = passed
        feed_data['criteria_details'] = criteria
        feed_data['status'] = 'OK'

        return feed_data

    except requests.Timeout:
        return {
            'status': 'ERROR',
            'reason': 'Timeout',
            'feed_id': feed_id,
            'url': url
        }
    except Exception as e:
        return {
            'status': 'ERROR',
            'reason': str(e),
            'feed_id': feed_id,
            'url': url
        }


def extract_feed_metadata(url, feed, feed_id):
    """Extract detailed metadata from feed"""
    # Basic info
    title = feed.feed.get('title', 'Unknown')
    pastor = feed.feed.get('itunes_author') or feed.feed.get('author', 'Unknown')
    description = feed.feed.get('subtitle') or feed.feed.get('summary', '')

    # Latest episode date
    days_since_update = None
    last_updated = None
    if feed.entries:
        try:
            latest = feed.entries[0]
            if hasattr(latest, 'published_parsed') and latest.published_parsed:
                pub_date = datetime(*latest.published_parsed[:6])
            elif hasattr(latest, 'updated_parsed') and latest.updated_parsed:
                pub_date = datetime(*latest.updated_parsed[:6])
            else:
                pub_date = None

            if pub_date:
                last_updated = pub_date.strftime('%Y-%m-%d')
                days_since_update = (datetime.now() - pub_date).days
        except Exception:
            pass

    # Average duration
    durations = []
    for entry in feed.entries[:5]:
        duration_str = entry.get('itunes_duration')
        if duration_str:
            duration_secs = parse_duration(duration_str)
            if duration_secs:
                durations.append(duration_secs / 60)

    avg_duration = sum(durations) / len(durations) if durations else None

    # Publishing interval
    pub_dates = []
    for entry in feed.entries[:10]:
        try:
            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                pub_date = datetime(*entry.published_parsed[:6])
            elif hasattr(entry, 'updated_parsed') and entry.updated_parsed:
                pub_date = datetime(*entry.updated_parsed[:6])
            else:
                continue
            pub_dates.append(pub_date)
        except Exception:
            continue

    publishing_interval = None
    if len(pub_dates) >= 2:
        intervals = []
        for i in range(len(pub_dates) - 1):
            days_between = (pub_dates[i] - pub_dates[i+1]).days
            if days_between > 0:
                intervals.append(days_between)
        if intervals:
            publishing_interval = sum(intervals) / len(intervals)

    # Metadata quality
    metadata_scores = []
    for entry in feed.entries[:5]:
        score = 0
        if entry.get('title') and len(entry.title) > 0:
            score += 1
        if entry.get('itunes_duration'):
            score += 1
        if entry.get('published_parsed') or entry.get('updated_parsed'):
            score += 1
        if entry.get('enclosures'):
            score += 1
        if entry.get('summary') or entry.get('description'):
            score += 1
        metadata_scores.append(score / 5)

    metadata_quality = sum(metadata_scores) / len(metadata_scores) if metadata_scores else 0

    # Recent episodes
    recent_episodes = []
    for entry in feed.entries[:3]:
        episode_duration = None
        duration_str = entry.get('itunes_duration')
        if duration_str:
            duration_secs = parse_duration(duration_str)
            if duration_secs:
                episode_duration = duration_secs / 60

        recent_episodes.append({
            'title': entry.get('title', 'Untitled'),
            'duration': episode_duration
        })

    return {
        'feed_id': feed_id,
        'url': url,
        'title': title,
        'pastor': pastor,
        'description': description[:200] if description else '',
        'last_updated': last_updated,
        'days_since_update': days_since_update,
        'avg_duration': avg_duration,
        'total_episodes': len(feed.entries),
        'metadata_quality': metadata_quality,
        'publishing_interval': publishing_interval,
        'recent_episodes': recent_episodes,
        'feed_object': feed  # Store for detailed analysis
    }


def classify_feed(feed_data):
    """Classify feed into tiers"""
    if feed_data.get('status') in ['REJECTED', 'ERROR']:
        return 'REJECTED'

    passed = feed_data.get('criteria_passed', 0)

    if passed == 5:
        return 'TIER_1'
    elif passed == 4:
        return 'TIER_2'
    elif passed >= 3:
        return 'TIER_3'
    else:
        return 'REJECTED'


def generate_detailed_analysis(feed_data):
    """Generate rich analysis for top feeds"""
    feed = feed_data.get('feed_object')
    if not feed:
        return {
            'content_type': 'Unknown',
            'current_series': 'Unknown',
            'why_excels': 'Meets quality criteria',
            'theological_focus': 'Unknown'
        }

    # Detect content type from titles
    titles_text = ' '.join([e.title.lower() for e in feed.entries[:10] if hasattr(e, 'title')])

    content_type = "General Teaching"
    if any(book in titles_text for book in ['genesis', 'exodus', 'psalms', 'isaiah', 'jeremiah']):
        content_type = "Old Testament Exposition"
    elif any(book in titles_text for book in ['matthew', 'mark', 'luke', 'john', 'romans', 'corinthians', 'revelation']):
        content_type = "New Testament Exposition"
    elif 'prophecy' in titles_text or 'end times' in titles_text or 'revelation' in titles_text:
        content_type = "Eschatology/Prophecy"
    elif 'verse by verse' in feed_data.get('description', '').lower():
        content_type = "Verse-by-verse Expository"

    # Detect current series
    recent_titles = [e.title for e in feed.entries[:5] if hasattr(e, 'title')]
    current_series = detect_series(recent_titles)

    # Generate excellence reasons
    excellence_reasons = []
    if feed_data.get('days_since_update') is not None:
        if feed_data['days_since_update'] == 0:
            excellence_reasons.append("Updated today!")
        elif feed_data['days_since_update'] <= 3:
            excellence_reasons.append("Very active publishing")

    if feed_data.get('avg_duration') and feed_data['avg_duration'] >= 45:
        excellence_reasons.append("Deep, long-form teaching")

    if feed_data.get('publishing_interval'):
        if feed_data['publishing_interval'] <= 2:
            excellence_reasons.append("Daily or near-daily updates")
        elif feed_data['publishing_interval'] <= 7:
            excellence_reasons.append("Consistent weekly schedule")

    if feed_data.get('metadata_quality', 0) >= 0.95:
        excellence_reasons.append("Exceptional metadata quality")

    why_excels = ", ".join(excellence_reasons) if excellence_reasons else "Meets all quality criteria"

    return {
        'content_type': content_type,
        'current_series': current_series,
        'why_excels': why_excels,
        'theological_focus': content_type
    }


def detect_series(titles):
    """Detect series name from episode titles"""
    if not titles:
        return "Various topics"

    # Look for common patterns
    for title in titles:
        if ':' in title:
            parts = title.split(':')
            if len(parts[0]) < 30:  # Likely a book/series name
                return parts[0].strip()

    # Look for repeated words (series names)
    words = defaultdict(int)
    for title in titles:
        title_words = title.split()[:3]  # First 3 words
        for word in title_words:
            if len(word) > 3 and word.isalpha():
                words[word] += 1

    if words:
        most_common = max(words.items(), key=lambda x: x[1])
        if most_common[1] >= 2:  # Appears in at least 2 titles
            return most_common[0]

    return "Various topics"


def format_schedule(interval_days):
    """Convert interval to readable schedule"""
    if interval_days is None:
        return "Unknown"
    if interval_days <= 1.5:
        return "Daily"
    elif interval_days <= 4:
        return "2-3 times per week"
    elif interval_days <= 8:
        return "Weekly"
    elif interval_days <= 15:
        return "Bi-weekly"
    else:
        return f"Every {interval_days:.0f} days"


def write_output_file(results, filename):
    """Write comprehensive analysis report"""
    with open(filename, 'w', encoding='utf-8') as f:
        # Header
        total_feeds = sum(len(tier) for tier in results.values())
        f.write("# VALIDATED SERMON FEEDS FOR DIGITAL PULPIT\n")
        f.write(f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write(f"# Total Feeds Validated: {total_feeds}\n\n")

        # SECTION 1: DETAILED ANALYSIS - TOP 10
        f.write("=" * 80 + "\n")
        f.write("# SECTION 1: DETAILED ANALYSIS - TOP 10 FEEDS\n")
        f.write("# These are the highest-quality feeds based on all criteria\n")
        f.write("=" * 80 + "\n\n")

        tier1_feeds = sorted(
            results['TIER_1'],
            key=lambda x: (
                x.get('days_since_update') if x.get('days_since_update') is not None else 999,
                -(x.get('avg_duration') if x.get('avg_duration') else 0)
            )
        )

        for i, feed in enumerate(tier1_feeds[:10], 1):
            analysis = generate_detailed_analysis(feed)

            f.write(f"### {i}. {feed['title']} ⭐⭐⭐⭐⭐\n\n")
            f.write(f"**Feed ID**: {feed['feed_id']}\n")
            f.write(f"**URL**: {feed['url']}\n")
            f.write(f"**Pastor/Speaker**: {feed['pastor']}\n\n")

            f.write("**Activity**:\n")
            f.write(f"- Last Updated: {feed['last_updated'] or 'Unknown'}")
            if feed.get('days_since_update') is not None:
                f.write(f" ({feed['days_since_update']} days ago)\n")
            else:
                f.write("\n")
            f.write(f"- Publishing Schedule: {format_schedule(feed.get('publishing_interval'))}\n")
            f.write(f"- Total Episodes: {feed.get('total_episodes', 0)}\n\n")

            f.write("**Content Profile**:\n")
            if feed.get('avg_duration'):
                f.write(f"- Average Duration: {feed['avg_duration']:.0f} minutes\n")
            else:
                f.write("- Average Duration: Unknown\n")
            f.write(f"- Current Series/Book: {analysis['current_series']}\n")
            f.write(f"- Content Type: {analysis['content_type']}\n\n")

            f.write("**Quality Metrics**:\n")
            f.write(f"- Metadata Completeness: {feed.get('metadata_quality', 0) * 100:.0f}%\n")
            f.write(f"- Publishing Consistency: {format_schedule(feed.get('publishing_interval'))}\n")
            f.write(f"- Criteria Passed: 5/5 ✅\n\n")

            f.write("**Description**:\n")
            f.write(f"{feed.get('description', 'No description')}...\n\n")

            # Recent episodes
            if feed.get('recent_episodes'):
                f.write("**Recent Episode Titles** (Last 3):\n")
                for j, episode in enumerate(feed['recent_episodes'], 1):
                    duration_str = f" - {episode['duration']:.0f} min" if episode.get('duration') else ""
                    f.write(f"{j}. {episode['title']}{duration_str}\n")
                f.write("\n")

            f.write(f"**Theological Focus**: {analysis['theological_focus']}\n\n")
            f.write(f"**Why This Feed Excels**: {analysis['why_excels']}\n\n")
            f.write("-" * 80 + "\n\n")

        # SECTION 2: COMPREHENSIVE LISTING
        f.write("\n" + "=" * 80 + "\n")
        f.write("# SECTION 2: COMPREHENSIVE FEED LISTING\n")
        f.write("# All feeds organized by tier\n")
        f.write("=" * 80 + "\n\n")

        # Write each tier
        for tier_name, tier_label in [
            ('TIER_1', 'TIER 1: EXCELLENT (All 5 Criteria Met)'),
            ('TIER_2', 'TIER 2: GOOD (4 of 5 Criteria Met)'),
            ('TIER_3', 'TIER 3: REVIEW NEEDED (3 of 5 Criteria Met)')
        ]:
            f.write(f"## {tier_label}\n\n")

            for feed in results[tier_name]:
                f.write(f"Feed ID: {feed['feed_id']}\n")
                f.write(f"URL: {feed['url']}\n")
                f.write(f"Church: {feed['title']}\n")
                f.write(f"Pastor: {feed['pastor']}\n")
                if feed.get('last_updated'):
                    days_str = f" ({feed['days_since_update']} days ago)" if feed.get('days_since_update') is not None else ""
                    f.write(f"Last Updated: {feed['last_updated']}{days_str}\n")
                if feed.get('avg_duration'):
                    f.write(f"Avg Duration: {feed['avg_duration']:.0f} minutes\n")
                f.write(f"Publishing: {format_schedule(feed.get('publishing_interval'))}\n")
                f.write(f"Episodes: {feed.get('total_episodes', 0)}\n")
                f.write(f"Description: {feed.get('description', 'No description')}\n")

                # Show which criteria passed/failed
                if 'criteria_details' in feed:
                    criteria_str = []
                    for k, v in feed['criteria_details'].items():
                        if v is True:
                            criteria_str.append(f"{k}:✅")
                        elif v == "NEEDS_REVIEW":
                            criteria_str.append(f"{k}:⚠️")
                        elif v == "UNKNOWN":
                            criteria_str.append(f"{k}:❓")
                        else:
                            criteria_str.append(f"{k}:❌")
                    f.write(f"Criteria: {' '.join(criteria_str)}\n")

                f.write("---\n\n")

        # REJECTED FEEDS
        f.write("## REJECTED FEEDS\n\n")
        for feed in results['REJECTED']:
            f.write(f"Feed ID: {feed['feed_id']}\n")
            f.write(f"Reason: {feed.get('reason', 'Failed quality criteria')}\n")
            f.write("---\n\n")

        # SUMMARY STATISTICS
        f.write("## SUMMARY STATISTICS\n\n")
        f.write(f"Total Feeds Discovered: {total_feeds}\n")
        f.write(f"Feeds Analyzed: {total_feeds}\n")
        f.write(f"Tier 1 (Excellent): {len(results['TIER_1'])}\n")
        f.write(f"Tier 2 (Good): {len(results['TIER_2'])}\n")
        f.write(f"Tier 3 (Review): {len(results['TIER_3'])}\n")
        f.write(f"Rejected: {len(results['REJECTED'])}\n\n")

        # Calculate average duration across all valid feeds
        all_durations = [f['avg_duration'] for f in results['TIER_1'] + results['TIER_2'] + results['TIER_3'] if f.get('avg_duration')]
        if all_durations:
            f.write(f"Average Duration: {sum(all_durations) / len(all_durations):.0f} minutes\n")

        # Most common publishing schedule
        schedules = [f.get('publishing_interval') for f in results['TIER_1'] + results['TIER_2'] + results['TIER_3'] if f.get('publishing_interval')]
        if schedules:
            avg_schedule = sum(schedules) / len(schedules)
            f.write(f"Most Common Publishing Schedule: {format_schedule(avg_schedule)}\n")


def main():
    """Main execution"""
    print("=" * 60)
    print("RSS FEED VALIDATION FOR DIGITAL PULPIT")
    print("=" * 60)
    print()

    # Load feed URLs from feeds.txt
    print("Loading feed URLs from feeds.txt...")
    with open('feeds.txt', 'r') as f:
        feed_urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]

    print(f"Found {len(feed_urls)} feeds to validate\n")

    # Initialize results
    results = {
        'TIER_1': [],
        'TIER_2': [],
        'TIER_3': [],
        'REJECTED': []
    }

    # Validate each feed
    print("Validating feeds...\n")
    for i, url in enumerate(feed_urls, 1):
        print(f"[{i}/{len(feed_urls)}] Validating feed...")
        feed_data = validate_feed(url)
        tier = classify_feed(feed_data)
        results[tier].append(feed_data)

        # Rate limiting
        time.sleep(0.5)

    print("\n" + "=" * 60)
    print("VALIDATION COMPLETE")
    print("=" * 60)
    print()

    # Write output file
    print("Generating report...")
    write_output_file(results, 'sermon_feeds.txt')

    # Print summary
    print("\nSUMMARY:")
    print(f"  Tier 1 (Excellent): {len(results['TIER_1'])}")
    print(f"  Tier 2 (Good):      {len(results['TIER_2'])}")
    print(f"  Tier 3 (Review):    {len(results['TIER_3'])}")
    print(f"  Rejected:           {len(results['REJECTED'])}")
    print(f"\nDetailed report written to: sermon_feeds.txt")
    print("\n✓ Analysis complete!")


if __name__ == '__main__':
    main()
