#!/usr/bin/env python3
"""
analyze_hope_keywords.py

Analyze which hope keywords are most common across the corpus.
"""

import sqlite3
import json
from collections import Counter
from typing import Dict, List


def get_all_raw_scores(db_path: str) -> List[Dict]:
    """Get raw_scores_json for all sermons."""
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    query = """
        SELECT br.video_id, v.title, br.hope_vs_fear, br.raw_scores_json
        FROM brain_results br
        JOIN videos v ON br.video_id = v.video_id
        WHERE br.hope_vs_fear >= 0.999
        ORDER BY br.hope_vs_fear DESC
    """

    results = []
    for row in con.execute(query):
        raw_scores = json.loads(row['raw_scores_json']) if row['raw_scores_json'] else {}
        results.append({
            'video_id': row['video_id'],
            'title': row['title'],
            'hope_vs_fear': row['hope_vs_fear'],
            'raw_scores': raw_scores
        })

    con.close()
    return results


def main():
    results = get_all_raw_scores('db/digital_pulpit.db')

    print("=" * 80)
    print("HOPE KEYWORD ANALYSIS - PERFECT SCORE SERMONS (hope_vs_fear = 1.000)")
    print("=" * 80)
    print()
    print(f"Analyzing {len(results)} sermons with perfect hope scores")
    print()

    # Aggregate hope keyword matches
    hope_keyword_counter = Counter()
    total_hope_count = 0

    for sermon in results:
        raw_scores = sermon['raw_scores']
        keyword_matches = raw_scores.get('keyword_matches', {})
        hope_matches = keyword_matches.get('hope', [])

        # Count each keyword
        for match in hope_matches:
            keyword = match.lower()
            hope_keyword_counter[keyword] += 1
            total_hope_count += 1

    print("TOP 20 HOPE KEYWORDS (by frequency)")
    print("-" * 80)
    print(f"{'Keyword':<20} {'Count':<10} {'% of Total':<15} {'Sermons':<10}")
    print("-" * 80)

    for keyword, count in hope_keyword_counter.most_common(20):
        pct_total = count / total_hope_count * 100 if total_hope_count > 0 else 0
        # Count how many unique sermons use this keyword
        sermons_with_keyword = sum(1 for s in results
                                   if any(m.lower() == keyword
                                         for m in s['raw_scores'].get('keyword_matches', {}).get('hope', [])))
        print(f"{keyword:<20} {count:<10} {pct_total:<14.1f}% {sermons_with_keyword:<10}")

    print()
    print(f"Total hope keywords matched: {total_hope_count}")
    print()

    # Show distribution
    print("HOPE KEYWORD COUNT DISTRIBUTION")
    print("-" * 80)

    hope_counts = []
    fear_counts = []

    con = sqlite3.connect('db/digital_pulpit.db')
    con.row_factory = sqlite3.Row

    for row in con.execute("SELECT raw_scores_json FROM brain_results"):
        raw_scores = json.loads(row['raw_scores_json']) if row['raw_scores_json'] else {}
        cat_counts = raw_scores.get('category_counts', {})
        hope_counts.append(cat_counts.get('hope', 0))
        fear_counts.append(cat_counts.get('fear', 0))

    con.close()

    # Bin the counts
    bins = [(0, 5), (5, 10), (10, 15), (15, 20), (20, 100)]

    print(f"{'Hope Count Range':<20} {'Sermons':<10} {'Percentage':<15}")
    print("-" * 80)

    for low, high in bins:
        count = sum(1 for h in hope_counts if low <= h < high)
        pct = count / len(hope_counts) * 100
        label = f"{low}-{high-1}" if high < 100 else f"{low}+"
        print(f"{label:<20} {count:<10} {pct:<14.1f}%")

    print()
    print("=" * 80)

    # Show some examples of perfect hope sermons
    print()
    print("SAMPLE PERFECT HOPE SERMONS (first 10)")
    print("-" * 80)

    for i, sermon in enumerate(results[:10]):
        raw_scores = sermon['raw_scores']
        cat_counts = raw_scores.get('category_counts', {})
        hope_count = cat_counts.get('hope', 0)
        fear_count = cat_counts.get('fear', 0)

        print(f"{i+1}. {sermon['title'][:60]}")
        print(f"   Video ID: {sermon['video_id']}")
        print(f"   Hope: {hope_count}, Fear: {fear_count}")
        print()

    print("=" * 80)


if __name__ == "__main__":
    main()
