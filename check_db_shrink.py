#!/usr/bin/env python3
"""
check_db_shrink.py
Ramp - release gate for aircraft.db

Compares a freshly generated aircraft.db against the previous release and exits
non-zero when it has shrunk enough to suggest an upstream data loss rather than
normal fleet churn. The workflow runs this before signing, so a bad build is
never published.

Why it exists: on 2026-10-12 the weekly build shipped with 334 Australian (VH-)
aircraft against 16,102 the week before, because tar1090-db's operator column
(ADS-B Exchange's `ownop`) stopped carrying Australian owners. Nothing failed and
every device that updated took the damaged copy.

Usage:
    python3 check_db_shrink.py NEW_DB [--previous PREV_DB]

Thresholds, all on active rows:
  - total may not drop by more than MAX_TOTAL_DROP
  - any registration prefix with at least PREFIX_MIN_ROWS rows in the previous
    build may not drop by more than MAX_PREFIX_DROP
  - FLOORS are absolute minimums, checked even with no previous build, so a run
    that compares against an already-damaged release still fails
"""

import argparse
import collections
import sqlite3
import sys

MAX_TOTAL_DROP  = 0.05
MAX_PREFIX_DROP = 0.20
PREFIX_MIN_ROWS = 500

# The app's home markets. Active rows in aircraft-db-20261005: total 322,575,
# N 303,973, VH 16,102.
FLOORS = {
    "TOTAL": 250_000,
    "N":     250_000,
    "VH":     10_000,
}


def prefix(registration: str) -> str:
    if "-" in registration:
        return registration.split("-", 1)[0]
    if registration.startswith("N"):
        return "N"
    return "other"


def counts(path: str) -> collections.Counter:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        try:
            rows = conn.execute("SELECT registration FROM aircraft WHERE active = 1")
        except sqlite3.OperationalError:
            rows = conn.execute("SELECT registration FROM aircraft")  # pre-`active` schema
        result = collections.Counter(prefix(r) for (r,) in rows)
    finally:
        conn.close()
    result["TOTAL"] = sum(result.values())
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Fail if aircraft.db shrank suspiciously")
    parser.add_argument("new_db")
    parser.add_argument("--previous", metavar="PATH", default=None)
    args = parser.parse_args()

    new = counts(args.new_db)
    old = counts(args.previous) if args.previous else None
    failures = []

    for key, floor in FLOORS.items():
        if new[key] < floor:
            failures.append(f"{key}: {new[key]:,} active rows, below the floor of {floor:,}")

    if old:
        if old["TOTAL"] and new["TOTAL"] < old["TOTAL"] * (1 - MAX_TOTAL_DROP):
            failures.append(f"TOTAL: {old['TOTAL']:,} -> {new['TOTAL']:,} "
                            f"(more than {MAX_TOTAL_DROP:.0%} drop)")
        for key, before in old.most_common():
            if key in ("TOTAL", "other") or before < PREFIX_MIN_ROWS:
                continue
            after = new[key]
            if after < before * (1 - MAX_PREFIX_DROP):
                failures.append(f"{key}-: {before:,} -> {after:,} "
                                f"(more than {MAX_PREFIX_DROP:.0%} drop)")

    print("prefix    previous        new")
    for key in ["TOTAL", "N", "VH", "C", "ZK"]:
        before = f"{old[key]:,}" if old else "-"
        print(f"{key:6} {before:>11} {new[key]:>10,}")

    if failures:
        print("\nREFUSING TO PUBLISH - aircraft.db shrank beyond the release gate:", file=sys.stderr)
        for f in failures:
            print(f"  - {f}", file=sys.stderr)
        print("If this is a genuine upstream change, re-run the workflow with "
              "allow_shrink enabled.", file=sys.stderr)
        return 1
    print("\nRelease gate passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
