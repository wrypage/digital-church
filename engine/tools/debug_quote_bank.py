#!/usr/bin/env python3
import argparse
import json
import sqlite3

from engine.config import DATABASE_PATH
from engine.quote_bank import get_quotes

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DATABASE_PATH, help="Path to SQLite DB")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--category", default="scripture_reference")
    ap.add_argument("--n", type=int, default=5)
    ap.add_argument("--distinct", action="store_true")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    quotes = get_quotes(conn, days=args.days, category=args.category, n=args.n, distinct_channels=args.distinct)

    print("\n=== get_quotes() raw output ===")
    print(json.dumps(quotes, indent=2))

    # Also print just the quote text field(s) in a readable way
    print("\n=== quote texts ===")
    for i, q in enumerate(quotes, start=1):
        if isinstance(q, dict):
            text = q.get("quote") or q.get("excerpt") or q.get("text") or ""
            print(f"\n[{i}] {text[:400]}")
        else:
            print(f"\n[{i}] {str(q)[:400]}")

    conn.close()

if __name__ == "__main__":
    main()
