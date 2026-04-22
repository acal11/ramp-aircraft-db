#!/usr/bin/env python3
"""
generate_aircraft_db.py
Ramp — Aircraft database generator

Downloads the tar1090-db aircraft database (ODC-BY), filters to
aircraft with a known registration and type code, and writes a SQLite
database to Ramp/Ramp/Resources/aircraft.db.

The resulting database has two indexes:
  - icao24       → used by LiveAircraftService (ADS-B transponder code)
  - registration → used by AircraftLookupService (typed-registration autofill)

The `active` column is 1 for all aircraft found in the current download.
When --previous-db is supplied, any icao24 present in the old DB but absent
from the new CSV is re-inserted with active=0 so historical sightings retain
their type and operator data.

Usage:
    python3 scripts/generate_aircraft_db.py                      # filtered (default)
    python3 scripts/generate_aircraft_db.py --all                # all with reg+type
    python3 scripts/generate_aircraft_db.py --previous-db PATH   # merge retired aircraft
    python3 scripts/generate_aircraft_db.py --output PATH        # write to custom path

Run from the root of the Ramp project directory.

Data source: https://github.com/wiedehopf/tar1090-db  (ODC-BY)
CSV snapshot URL:
    https://raw.githubusercontent.com/wiedehopf/tar1090-db/refs/heads/csv/aircraft.csv.gz

CSV format: gzipped, semicolon-delimited, NO header row.
Column order:
    0  icao24        (hex, lowercase)
    1  registration
    2  typecode      (ICAO type designator, e.g. B738)
    3  dbFlags       (bitmask — bit 1 = military, bit 2 = interesting, bit 3 = PIA, bit 4 = LADD)
    4  type_long     (human-readable model name, e.g. "BOEING 737-800")
    5  year          (year of manufacture, may be empty)
    6  operator      (airline/operator name, may be empty)
"""

import argparse
import csv
import gzip
import io
import os
import sqlite3
import sys
import urllib.request
from datetime import datetime, timezone


# ── Paths ──────────────────────────────────────────────────────────────────────

SCRIPTS_DIR  = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUTPUT = os.path.join(SCRIPTS_DIR, "../Ramp/Resources/aircraft.db")
LOCAL_CACHE  = os.path.join(SCRIPTS_DIR, "aircraft-database.csv")  # optional local copy (decompressed)

TAR1090_URL  = "https://raw.githubusercontent.com/wiedehopf/tar1090-db/refs/heads/csv/aircraft.csv.gz"

# Column indices in the semicolon-delimited CSV
COL_ICAO24       = 0
COL_REGISTRATION = 1
COL_TYPECODE     = 2
COL_DB_FLAGS     = 3
COL_TYPE_LONG    = 4
COL_YEAR         = 5
COL_OPERATOR     = 6

# dbFlags bitmask
FLAG_MILITARY    = 1


# ── Filter criteria ────────────────────────────────────────────────────────────
#
# Goal: include the aircraft a planespotter would realistically see at a
# commercial airport, while keeping the bundle size reasonable.
#
# Include a row when ALL of the following are true:
#   1. registration is non-empty
#   2. typecode is non-empty
#   3. operator is non-empty   (filters to fleet/commercial aircraft)
#
# Pass --all to skip criterion 3 (adds private/GA aircraft; roughly 3× larger).
# Estimated row counts:  --all ≈ 300–400k rows,  default ≈ 30–60k rows.
# Estimated SQLite size: --all ≈ 30–50 MB,        default ≈ 5–10 MB.

def should_include(row: list[str], include_all: bool) -> bool:
    if len(row) <= COL_OPERATOR:
        return False

    registration = row[COL_REGISTRATION].strip()
    typecode     = row[COL_TYPECODE].strip()
    operator_    = row[COL_OPERATOR].strip()

    if not registration or not typecode:
        return False
    if not include_all and not operator_:
        return False
    return True


# ── Download ───────────────────────────────────────────────────────────────────

def download_csv() -> list[list[str]]:
    if os.path.exists(LOCAL_CACHE):
        print(f"Using local cache: {LOCAL_CACHE}")
        with open(LOCAL_CACHE, encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=";")
            return list(reader)

    print(f"Downloading {TAR1090_URL} …")
    try:
        req = urllib.request.Request(
            TAR1090_URL,
            headers={"User-Agent": "Ramp/1.0 (planespotter app)"}
        )
        with urllib.request.urlopen(req, timeout=120) as resp:
            compressed = resp.read()

        print(f"  Downloaded {len(compressed) / (1024*1024):.1f} MB (compressed)")

        content = gzip.decompress(compressed).decode("utf-8")
        print(f"  Decompressed to {len(content) / (1024*1024):.1f} MB")

        # Cache decompressed CSV for subsequent runs during development
        with open(LOCAL_CACHE, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"  Cached to {LOCAL_CACHE}")

        reader = csv.reader(io.StringIO(content), delimiter=";")
        return list(reader)

    except Exception as e:
        print(f"  Error: {e}", file=sys.stderr)
        print("Tip: place a decompressed local copy at scripts/aircraft-database.csv", file=sys.stderr)
        sys.exit(1)


# ── Previous DB loading (for retired-aircraft preservation) ────────────────────

def load_previous_db(path: str) -> dict:
    """Returns {icao24: {registration, typecode, model, operator, active}} for all rows."""
    if not os.path.exists(path):
        print(f"  Warning: --previous-db path not found: {path}", file=sys.stderr)
        return {}
    conn = sqlite3.connect(path)
    cur  = conn.cursor()
    try:
        cur.execute("SELECT icao24, registration, typecode, model, operator, active FROM aircraft")
    except sqlite3.OperationalError:
        # Old schema without active column — treat all as active
        cur.execute("SELECT icao24, registration, typecode, model, operator FROM aircraft")
        result = {}
        for row in cur.fetchall():
            result[row[0]] = {
                "registration": row[1], "typecode": row[2],
                "model": row[3], "operator": row[4], "active": 1
            }
        conn.close()
        return result
    result = {}
    for row in cur.fetchall():
        result[row[0]] = {
            "registration": row[1], "typecode": row[2],
            "model": row[3], "operator": row[4], "active": row[5]
        }
    conn.close()
    return result


# ── Database ───────────────────────────────────────────────────────────────────

CREATE_TABLE = """
CREATE TABLE aircraft (
    icao24          TEXT NOT NULL,
    registration    TEXT NOT NULL,
    typecode        TEXT NOT NULL,
    model           TEXT NOT NULL DEFAULT '',
    operator        TEXT NOT NULL DEFAULT '',
    active          INTEGER NOT NULL DEFAULT 1
);
"""

CREATE_INDEX_ICAO24 = "CREATE UNIQUE INDEX idx_icao24 ON aircraft (icao24);"
CREATE_INDEX_REG    = "CREATE INDEX idx_registration ON aircraft (registration);"


def build_database(rows: list[list[str]], output_path: str, include_all: bool, prev_rows: dict) -> int:
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    # Remove stale database
    if os.path.exists(output_path):
        os.remove(output_path)

    conn = sqlite3.connect(output_path)
    cur  = conn.cursor()
    cur.executescript(CREATE_TABLE)

    inserted   = 0
    skipped    = 0
    duplicates = 0
    retired    = 0
    seen_icao  = set()

    for row in rows:
        if not should_include(row, include_all):
            skipped += 1
            continue

        icao24 = row[COL_ICAO24].strip().lower()
        if not icao24 or icao24 in seen_icao:
            duplicates += 1
            continue
        seen_icao.add(icao24)

        registration = row[COL_REGISTRATION].strip().upper()
        typecode     = row[COL_TYPECODE].strip().upper()
        type_long    = row[COL_TYPE_LONG].strip() if len(row) > COL_TYPE_LONG else ""
        operator_    = row[COL_OPERATOR].strip() if len(row) > COL_OPERATOR else ""

        cur.execute(
            "INSERT INTO aircraft VALUES (?,?,?,?,?,?)",
            (icao24, registration, typecode, type_long, operator_, 1),
        )
        inserted += 1

        if inserted % 10_000 == 0:
            print(f"  … {inserted:,} rows inserted")

    # Preserve retired aircraft from the previous DB (active=0)
    if prev_rows:
        for icao24, prev in prev_rows.items():
            if icao24 in seen_icao:
                continue  # still in the new data — already inserted above
            # Re-insert with active=0 to preserve history
            cur.execute(
                "INSERT OR IGNORE INTO aircraft VALUES (?,?,?,?,?,?)",
                (
                    icao24,
                    prev["registration"],
                    prev["typecode"],
                    prev["model"],
                    prev["operator"],
                    0,  # inactive
                ),
            )
            retired += 1
        if retired:
            print(f"  … {retired:,} retired aircraft preserved (active=0)")

    conn.commit()

    print("Building indexes …")
    cur.executescript(CREATE_INDEX_ICAO24 + "\n" + CREATE_INDEX_REG)
    conn.commit()
    conn.close()

    print(f"  Skipped {skipped:,} rows, {duplicates:,} duplicates")
    return inserted


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Ramp aircraft.db from tar1090-db")
    parser.add_argument("--all", action="store_true",
                        help="Include all aircraft with registration + typecode (no operator filter)")
    parser.add_argument("--previous-db", metavar="PATH", default=None,
                        help="Path to the previous aircraft.db; retired aircraft are preserved with active=0")
    parser.add_argument("--output", metavar="PATH", default=DEFAULT_OUTPUT,
                        help="Output path for the generated aircraft.db")
    args = parser.parse_args()

    include_all = args.all
    output_path = os.path.abspath(args.output)

    if include_all:
        print("Mode: --all  (all aircraft with registration + typecode)")
    else:
        print("Mode: default  (commercial/fleet aircraft with operator populated)")
        print("      Pass --all to include private/GA aircraft (~5-10× larger)")

    prev_rows: dict = {}
    if args.previous_db:
        print(f"Loading previous DB: {args.previous_db}")
        prev_rows = load_previous_db(args.previous_db)
        print(f"  {len(prev_rows):,} entries in previous DB")

    rows = download_csv()
    print(f"Loaded {len(rows):,} rows from CSV")

    count = build_database(rows, output_path, include_all, prev_rows)

    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    print(f"\n✓ Written {count:,} aircraft to {output_path}")
    print(f"  File size: {size_mb:.1f} MB")
    print()
    print("Next steps:")
    print("  1. Add aircraft.db to the Xcode project (Ramp target membership)")
    print("  2. Verify a sample lookup:")
    print(f"     sqlite3 {output_path} \"SELECT * FROM aircraft WHERE registration = 'VH-OQA';\"")
    print(f"     sqlite3 {output_path} \"SELECT * FROM aircraft WHERE icao24 = '7c6b28';\"")


if __name__ == "__main__":
    main()
