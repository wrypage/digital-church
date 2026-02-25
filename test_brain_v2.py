#!/usr/bin/env python3
"""Test Brain v2 with 2 transcripts."""

import sys
sys.path.insert(0, '.')

from engine import brain

print("="*80)
print("SMOKE TEST: Running Brain v2 on 2 transcripts")
print("="*80)

# Run with limit=2
brain.run(limit=2)

print("\n" + "="*80)
print("SMOKE TEST COMPLETE")
print("="*80)
