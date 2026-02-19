#!/usr/bin/env python3
"""
Transcript Quality Audit Tool

Performs lightweight quality checks on transcripts in the database without
invoking Brain. Generates sample exports and health metrics to identify problems.

Usage:
    python -m engine.tools.transcript_quality_audit
    python -m engine.tools.transcript_quality_audit --db db/digital_pulpit.db --out reports/quality
"""

import os
import sys
import json
import re
import sqlite3
import argparse
import csv
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from collections import Counter, defaultdict

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


class TranscriptQualityAuditor:
    """Audit transcript quality and generate reports."""

    def __init__(self, db_path: str, output_dir: str, sample_size: int = 5):
        self.db_path = db_path
        self.output_dir = Path(output_dir)
        self.sample_size = sample_size
        self.findings = []
        self.suspect_transcripts = []

        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def connect_db(self) -> sqlite3.Connection:
        """Connect to database in read-only mode with timeout."""
        # Try read-only mode first
        try:
            conn = sqlite3.connect(f'file:{self.db_path}?mode=ro', uri=True, timeout=5.0)
        except Exception:
            # Fallback to regular connection
            conn = sqlite3.connect(self.db_path, timeout=5.0)

        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def get_sample_transcripts(self, conn: sqlite3.Connection) -> List[Dict]:
        """Get representative sample of transcripts."""
        samples = []

        # Non-empty filter
        non_empty_filter = "full_text IS NOT NULL AND trim(full_text) != ''"

        # Get shortest 5
        cursor = conn.execute(f"""
            SELECT transcript_id, video_id, full_text, segments_json, language,
                   word_count, transcript_provider, transcript_model,
                   transcript_version, transcribed_at,
                   COALESCE(word_count, length(full_text)) as size_metric
            FROM transcripts
            WHERE {non_empty_filter}
            ORDER BY size_metric ASC
            LIMIT ?
        """, (self.sample_size,))
        for row in cursor.fetchall():
            samples.append(self._row_to_dict(row, 'shortest'))

        # Get longest 5
        cursor = conn.execute(f"""
            SELECT transcript_id, video_id, full_text, segments_json, language,
                   word_count, transcript_provider, transcript_model,
                   transcript_version, transcribed_at,
                   COALESCE(word_count, length(full_text)) as size_metric
            FROM transcripts
            WHERE {non_empty_filter}
            ORDER BY size_metric DESC
            LIMIT ?
        """, (self.sample_size,))
        for row in cursor.fetchall():
            samples.append(self._row_to_dict(row, 'longest'))

        # Get middle 5 (around median)
        cursor = conn.execute(f"""
            WITH ranked AS (
                SELECT transcript_id, video_id, full_text, segments_json, language,
                       word_count, transcript_provider, transcript_model,
                       transcript_version, transcribed_at,
                       COALESCE(word_count, length(full_text)) as size_metric,
                       ROW_NUMBER() OVER (ORDER BY COALESCE(word_count, length(full_text))) as rn,
                       COUNT(*) OVER () as total
                FROM transcripts
                WHERE {non_empty_filter}
            )
            SELECT transcript_id, video_id, full_text, segments_json, language,
                   word_count, transcript_provider, transcript_model,
                   transcript_version, transcribed_at, size_metric
            FROM ranked
            WHERE rn BETWEEN (total/2 - ?) AND (total/2 + ?)
            LIMIT ?
        """, (self.sample_size // 2, self.sample_size // 2, self.sample_size))
        for row in cursor.fetchall():
            samples.append(self._row_to_dict(row, 'middle'))

        # Get random 5
        cursor = conn.execute(f"""
            SELECT transcript_id, video_id, full_text, segments_json, language,
                   word_count, transcript_provider, transcript_model,
                   transcript_version, transcribed_at,
                   COALESCE(word_count, length(full_text)) as size_metric
            FROM transcripts
            WHERE {non_empty_filter}
            ORDER BY RANDOM()
            LIMIT ?
        """, (self.sample_size,))
        for row in cursor.fetchall():
            samples.append(self._row_to_dict(row, 'random'))

        return samples

    def _row_to_dict(self, row: Tuple, sample_group: str) -> Dict:
        """Convert database row to dictionary."""
        return {
            'transcript_id': row[0],
            'video_id': row[1],
            'full_text': row[2],
            'segments_json': row[3],
            'language': row[4],
            'word_count': row[5],
            'transcript_provider': row[6],
            'transcript_model': row[7],
            'transcript_version': row[8],
            'transcribed_at': row[9],
            'size_metric': row[10],
            'sample_group': sample_group
        }

    def compute_metrics(self, conn: sqlite3.Connection) -> Dict:
        """Compute basic health metrics."""
        metrics = {}

        # Total transcripts
        cursor = conn.execute("SELECT COUNT(*) FROM transcripts")
        metrics['total_transcripts'] = cursor.fetchone()[0]

        # Empty/NULL text
        cursor = conn.execute("""
            SELECT COUNT(*) FROM transcripts
            WHERE full_text IS NULL OR trim(full_text) = ''
        """)
        metrics['empty_text_count'] = cursor.fetchone()[0]

        # NULL word_count
        cursor = conn.execute("SELECT COUNT(*) FROM transcripts WHERE word_count IS NULL")
        metrics['null_word_count'] = cursor.fetchone()[0]

        # Word count statistics
        cursor = conn.execute("""
            SELECT MIN(word_count), AVG(word_count), MAX(word_count)
            FROM transcripts
            WHERE word_count IS NOT NULL
        """)
        min_wc, avg_wc, max_wc = cursor.fetchone()
        metrics['word_count_min'] = min_wc
        metrics['word_count_avg'] = round(avg_wc, 2) if avg_wc else None
        metrics['word_count_max'] = max_wc

        # Median word count
        cursor = conn.execute("""
            WITH ranked AS (
                SELECT word_count,
                       ROW_NUMBER() OVER (ORDER BY word_count) as rn,
                       COUNT(*) OVER () as total
                FROM transcripts
                WHERE word_count IS NOT NULL
            )
            SELECT word_count FROM ranked WHERE rn = (total + 1) / 2
        """)
        result = cursor.fetchone()
        metrics['word_count_median'] = result[0] if result else None

        # Top 10 longest
        cursor = conn.execute("""
            SELECT transcript_id, video_id, word_count
            FROM transcripts
            WHERE word_count IS NOT NULL
            ORDER BY word_count DESC
            LIMIT 10
        """)
        metrics['longest_transcripts'] = [
            {'transcript_id': row[0], 'video_id': row[1], 'word_count': row[2]}
            for row in cursor.fetchall()
        ]

        # Top 10 shortest
        cursor = conn.execute("""
            SELECT transcript_id, video_id, word_count
            FROM transcripts
            WHERE word_count IS NOT NULL
            ORDER BY word_count ASC
            LIMIT 10
        """)
        metrics['shortest_transcripts'] = [
            {'transcript_id': row[0], 'video_id': row[1], 'word_count': row[2]}
            for row in cursor.fetchall()
        ]

        # Check for duplicate video_ids
        cursor = conn.execute("""
            SELECT video_id, COUNT(*) as cnt
            FROM transcripts
            GROUP BY video_id
            HAVING cnt > 1
        """)
        duplicates = cursor.fetchall()
        metrics['duplicate_video_ids'] = [
            {'video_id': row[0], 'count': row[1]}
            for row in duplicates
        ]

        # Language distribution
        cursor = conn.execute("""
            SELECT language, COUNT(*) as cnt
            FROM transcripts
            GROUP BY language
            ORDER BY cnt DESC
        """)
        metrics['language_distribution'] = {
            row[0] or 'NULL': row[1]
            for row in cursor.fetchall()
        }

        # Provider distribution
        cursor = conn.execute("""
            SELECT transcript_provider, COUNT(*) as cnt
            FROM transcripts
            GROUP BY transcript_provider
            ORDER BY cnt DESC
        """)
        metrics['provider_distribution'] = {
            row[0] or 'NULL': row[1]
            for row in cursor.fetchall()
        }

        # Model distribution
        cursor = conn.execute("""
            SELECT transcript_model, COUNT(*) as cnt
            FROM transcripts
            GROUP BY transcript_model
            ORDER BY cnt DESC
        """)
        metrics['model_distribution'] = {
            row[0] or 'NULL': row[1]
            for row in cursor.fetchall()
        }

        # Version distribution
        cursor = conn.execute("""
            SELECT transcript_version, COUNT(*) as cnt
            FROM transcripts
            GROUP BY transcript_version
            ORDER BY cnt DESC
        """)
        metrics['version_distribution'] = {
            row[0] or 'NULL': row[1]
            for row in cursor.fetchall()
        }

        return metrics

    def check_quality(self, transcript: Dict) -> Dict:
        """Perform quality checks on a single transcript."""
        checks = {}
        text = transcript.get('full_text', '')
        word_count = transcript.get('word_count')

        # Character count
        checks['char_count'] = len(text)

        # Estimated word count if NULL
        if word_count is None:
            checks['word_count_estimated'] = len(text.split())
        else:
            checks['word_count_estimated'] = word_count

        # Repetition detection
        checks['repetition_score'], checks['top_repeated_phrase'] = self._detect_repetition(text)

        # Truncation indicators
        checks['truncated'] = self._check_truncation(text)

        # Noise indicators
        checks['timestamp_count'] = len(re.findall(r'\b\d{1,2}:\d{2}\b', text))
        checks['bracket_artifact_count'] = len(re.findall(r'\[(?:Music|Applause|Laughter|Inaudible)\]', text, re.I))
        checks['sponsor_keywords'] = bool(re.search(r'\b(subscribe|like and subscribe|patreon|sponsor)\b', text, re.I))

        # Encoding issues
        checks['non_ascii_ratio'] = sum(1 for c in text if ord(c) > 127) / max(len(text), 1)

        # Language mismatch (simple heuristic)
        checks['language_mismatch'] = self._check_language_mismatch(transcript.get('language'), text)

        return checks

    def _detect_repetition(self, text: str, phrase_len: int = 4) -> Tuple[float, Optional[str]]:
        """Detect repetitive phrases in text."""
        if not text:
            return 0.0, None

        words = text.lower().split()
        if len(words) < phrase_len:
            return 0.0, None

        # Count all n-grams
        ngrams = []
        for i in range(len(words) - phrase_len + 1):
            ngrams.append(' '.join(words[i:i + phrase_len]))

        if not ngrams:
            return 0.0, None

        counter = Counter(ngrams)
        most_common = counter.most_common(1)[0]
        top_phrase, count = most_common

        # Repetition score: what % of ngrams is the top repeated phrase?
        repetition_score = (count / len(ngrams)) * 100

        return round(repetition_score, 2), top_phrase if count > 2 else None

    def _check_truncation(self, text: str) -> bool:
        """Check if text appears truncated."""
        if not text:
            return False

        # Check if ends mid-sentence
        last_char = text.strip()[-1] if text.strip() else ''
        ends_mid_sentence = last_char not in '.?!\'"'

        # Only flag if both mid-sentence AND reasonably long
        return ends_mid_sentence and len(text) > 1000

    def _check_language_mismatch(self, declared_lang: Optional[str], text: str) -> bool:
        """Simple heuristic to detect language mismatch."""
        if not declared_lang or declared_lang == 'en':
            return False

        # If language is not English but text is mostly ASCII with common English words
        if declared_lang != 'en':
            ascii_ratio = sum(1 for c in text if ord(c) < 128) / max(len(text), 1)
            common_english = ['the', 'and', 'is', 'to', 'of', 'in', 'that', 'it', 'for']
            english_word_count = sum(text.lower().count(f' {word} ') for word in common_english)

            # If mostly ASCII and has many common English words, might be mismatch
            return ascii_ratio > 0.95 and english_word_count > 10

        return False

    def identify_suspects(self, samples: List[Dict], metrics: Dict) -> List[Dict]:
        """Identify suspect transcripts based on quality checks."""
        suspects = []

        # Calculate 99th percentile for outlier detection
        all_word_counts = [s['word_count'] for s in samples if s.get('word_count')]
        if all_word_counts:
            all_word_counts.sort()
            p99_index = int(len(all_word_counts) * 0.99)
            p99_threshold = all_word_counts[p99_index] if p99_index < len(all_word_counts) else 20000
        else:
            p99_threshold = 20000

        for sample in samples:
            reasons = []
            quality = self.check_quality(sample)

            # Empty text
            if not sample.get('full_text') or not sample['full_text'].strip():
                reasons.append('empty_text')

            # Very short
            if quality['word_count_estimated'] < 300:
                reasons.append(f"too_short ({quality['word_count_estimated']} words)")

            # Very long outlier
            if quality['word_count_estimated'] > p99_threshold:
                reasons.append(f"outlier_long ({quality['word_count_estimated']} words, p99={p99_threshold})")

            # High repetition
            if quality['repetition_score'] > 30:
                reasons.append(f"high_repetition ({quality['repetition_score']}%)")

            # Truncated
            if quality['truncated']:
                reasons.append('truncated')

            # Heavy timestamps
            if quality['timestamp_count'] > 20:
                reasons.append(f"timestamps ({quality['timestamp_count']})")

            # Heavy bracket artifacts
            if quality['bracket_artifact_count'] > 10:
                reasons.append(f"bracket_artifacts ({quality['bracket_artifact_count']})")

            # Language mismatch
            if quality['language_mismatch']:
                reasons.append('language_mismatch')

            if reasons:
                suspects.append({
                    'transcript_id': sample['transcript_id'],
                    'video_id': sample['video_id'],
                    'word_count': sample.get('word_count'),
                    'reasons': reasons
                })

        return suspects

    def write_sample_csv(self, samples: List[Dict]):
        """Write sample transcripts to CSV."""
        csv_path = self.output_dir / 'transcript_quality_sample.csv'

        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                'transcript_id', 'video_id', 'word_count', 'chars', 'language',
                'transcript_provider', 'transcript_model', 'transcript_version',
                'transcribed_at', 'sample_group', 'full_text'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for sample in samples:
                quality = self.check_quality(sample)
                writer.writerow({
                    'transcript_id': sample['transcript_id'],
                    'video_id': sample['video_id'],
                    'word_count': sample.get('word_count'),
                    'chars': quality['char_count'],
                    'language': sample.get('language'),
                    'transcript_provider': sample.get('transcript_provider'),
                    'transcript_model': sample.get('transcript_model'),
                    'transcript_version': sample.get('transcript_version'),
                    'transcribed_at': sample.get('transcribed_at'),
                    'sample_group': sample.get('sample_group'),
                    'full_text': sample.get('full_text', '')[:5000]  # Truncate for CSV
                })

        print(f"✓ Wrote sample CSV: {csv_path}")

    def write_metrics_json(self, metrics: Dict):
        """Write metrics to JSON."""
        json_path = self.output_dir / 'transcript_quality_metrics.json'

        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(metrics, f, indent=2)

        print(f"✓ Wrote metrics JSON: {json_path}")

    def write_findings_md(self, metrics: Dict, suspects: List[Dict]):
        """Write findings to Markdown."""
        md_path = self.output_dir / 'transcript_quality_findings.md'

        with open(md_path, 'w', encoding='utf-8') as f:
            f.write("# Transcript Quality Audit Report\n\n")
            f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # Summary
            f.write("## Summary Metrics\n\n")
            f.write(f"- **Total Transcripts:** {metrics['total_transcripts']:,}\n")
            f.write(f"- **Empty/NULL Text:** {metrics['empty_text_count']:,}\n")
            f.write(f"- **NULL Word Count:** {metrics['null_word_count']:,}\n\n")

            f.write("### Word Count Statistics\n\n")
            f.write(f"- **Min:** {metrics['word_count_min']:,}\n")
            f.write(f"- **Median:** {metrics['word_count_median']:,}\n")
            f.write(f"- **Average:** {metrics['word_count_avg']:,.2f}\n")
            f.write(f"- **Max:** {metrics['word_count_max']:,}\n\n")

            # Distributions
            f.write("### Distributions\n\n")
            f.write("**Language:**\n")
            for lang, count in metrics['language_distribution'].items():
                f.write(f"- {lang}: {count:,}\n")

            f.write("\n**Provider:**\n")
            for provider, count in metrics['provider_distribution'].items():
                f.write(f"- {provider}: {count:,}\n")

            f.write("\n**Model:**\n")
            for model, count in metrics['model_distribution'].items():
                f.write(f"- {model}: {count:,}\n")

            # Anomalies
            f.write("\n## Anomalies\n\n")

            if metrics['duplicate_video_ids']:
                f.write("### ⚠️ Duplicate Video IDs\n\n")
                for dup in metrics['duplicate_video_ids']:
                    f.write(f"- `{dup['video_id']}`: {dup['count']} transcripts\n")
                f.write("\n")
            else:
                f.write("✓ No duplicate video_ids found\n\n")

            if metrics['empty_text_count'] > 0:
                f.write(f"### ⚠️ Empty Transcripts: {metrics['empty_text_count']}\n\n")

            # Suspects
            if suspects:
                f.write(f"\n## Suspect Transcripts ({len(suspects)})\n\n")
                f.write("| Transcript ID | Video ID | Word Count | Issues |\n")
                f.write("|---------------|----------|------------|--------|\n")

                for suspect in suspects[:50]:  # Limit to 50
                    reasons_str = ', '.join(suspect['reasons'])
                    f.write(f"| {suspect['transcript_id']} | `{suspect['video_id']}` | "
                           f"{suspect['word_count'] or 'N/A'} | {reasons_str} |\n")

                if len(suspects) > 50:
                    f.write(f"\n*... and {len(suspects) - 50} more*\n")
            else:
                f.write("\n✓ No suspect transcripts identified\n")

            # Recommendations
            f.write("\n## Recommended Actions\n\n")

            if metrics['empty_text_count'] > 0:
                f.write(f"1. **Investigate {metrics['empty_text_count']} empty transcripts** - "
                       "Check if these are failed transcriptions or corrupted data\n")

            if suspects:
                short_suspects = [s for s in suspects if any('too_short' in r for r in s['reasons'])]
                if short_suspects:
                    f.write(f"2. **Review {len(short_suspects)} very short transcripts** - "
                           "May indicate partial transcriptions or non-sermon content\n")

                rep_suspects = [s for s in suspects if any('repetition' in r for r in s['reasons'])]
                if rep_suspects:
                    f.write(f"3. **Check {len(rep_suspects)} high-repetition transcripts** - "
                           "Possible loop artifacts or encoding issues\n")

            if metrics['duplicate_video_ids']:
                f.write(f"4. **Clean up {len(metrics['duplicate_video_ids'])} duplicate video_ids** - "
                       "Should have unique constraint\n")

            f.write("\n---\n\n")
            f.write("*For detailed transcript samples, see `transcript_quality_sample.csv`*\n")
            f.write("*For full metrics, see `transcript_quality_metrics.json`*\n")

        print(f"✓ Wrote findings report: {md_path}")

    def run(self):
        """Run full audit."""
        print("=" * 80)
        print("TRANSCRIPT QUALITY AUDIT")
        print("=" * 80)
        print(f"Database: {self.db_path}")
        print(f"Output: {self.output_dir}")
        print(f"Sample size: {self.sample_size} per group\n")

        conn = self.connect_db()

        try:
            # Get sample transcripts
            print("Sampling transcripts...")
            samples = self.get_sample_transcripts(conn)
            print(f"✓ Collected {len(samples)} sample transcripts\n")

            # Compute metrics
            print("Computing metrics...")
            metrics = self.compute_metrics(conn)
            print(f"✓ Computed metrics for {metrics['total_transcripts']} transcripts\n")

            # Identify suspects
            print("Identifying suspect transcripts...")
            suspects = self.identify_suspects(samples, metrics)
            print(f"✓ Found {len(suspects)} suspect transcripts\n")

            # Write outputs
            print("Writing reports...")
            self.write_sample_csv(samples)
            self.write_metrics_json(metrics)
            self.write_findings_md(metrics, suspects)

            print("\n" + "=" * 80)
            print("AUDIT COMPLETE")
            print("=" * 80)
            print(f"\nReports saved to: {self.output_dir}")
            print(f"- Sample CSV: transcript_quality_sample.csv")
            print(f"- Metrics JSON: transcript_quality_metrics.json")
            print(f"- Findings Report: transcript_quality_findings.md")

        finally:
            conn.close()


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Audit transcript quality and generate reports'
    )
    parser.add_argument(
        '--db',
        default='db/digital_pulpit.db',
        help='Path to SQLite database (default: db/digital_pulpit.db)'
    )
    parser.add_argument(
        '--out',
        default='reports/quality',
        help='Output directory for reports (default: reports/quality)'
    )
    parser.add_argument(
        '--sample-size',
        type=int,
        default=5,
        help='Number of samples per group (default: 5)'
    )

    args = parser.parse_args()

    auditor = TranscriptQualityAuditor(
        db_path=args.db,
        output_dir=args.out,
        sample_size=args.sample_size
    )

    auditor.run()


if __name__ == '__main__':
    main()
