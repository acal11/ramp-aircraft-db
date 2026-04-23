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
DEFAULT_OUTPUT = os.path.join(SCRIPTS_DIR, "aircraft.db")
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


# ── Brand mapping ──────────────────────────────────────────────────────────────
#
# Maps the raw legal operator name (as it appears in the source CSV) to the
# consumer-facing brand name.  Extend this list as needed; any operator not
# listed will fall back to to_display_name() title-casing.

BRAND_MAP: dict[str, str] = {
    # ── Australia ──────────────────────────────────────────────────────────────
    "QANTAS AIRWAYS LIMITED":                          "Qantas",
    "JETSTAR AIRWAYS PTY LIMITED":                     "Jetstar",
    "JETSTAR ASIA AIRWAYS PTE LTD":                    "Jetstar Asia",
    "VIRGIN AUSTRALIA AIRLINES PTY LTD":               "Virgin Australia",
    "VIRGIN AUSTRALIA INTERNATIONAL AIRLINES PTY LTD": "Virgin Australia",
    "VIRGIN AUSTRALIA REGIONAL AIRLINES PTY LTD":      "Virgin Australia",
    "REGIONAL EXPRESS PTY LTD":                        "Rex",
    "ALLIANCE AIRLINES PTY LIMITED":                   "Alliance Airlines",
    "BONZA AVIATION PTY LTD":                          "Bonza",
    "SKYTRANS AIRLINES PTY LTD":                       "Skytrans",
    "AIRNORTH REGIONAL PTY LTD":                       "Airnorth",
    "FLIGHT TRAINING ADELAIDE PTY LTD":                "Flight Training Adelaide",
    "ROYAL FLYING DOCTOR SERVICE OF AUSTRALIA":        "Royal Flying Doctor Service",
    # ── New Zealand ────────────────────────────────────────────────────────────
    "AIR NEW ZEALAND LIMITED":                         "Air New Zealand",
    "SOUNDS AIR LIMITED":                              "Sounds Air",
    # ── United States ──────────────────────────────────────────────────────────
    "UNITED AIRLINES INC":                             "United Airlines",
    "DELTA AIR LINES INC":                             "Delta Air Lines",
    "AMERICAN AIRLINES INC":                           "American Airlines",
    "SOUTHWEST AIRLINES CO":                           "Southwest Airlines",
    "JETBLUE AIRWAYS CORP":                            "JetBlue",
    "ALASKA AIRLINES INC":                             "Alaska Airlines",
    "SKYWEST AIRLINES INC":                            "SkyWest Airlines",
    "SKYWEST LEASING INC":                             "SkyWest Airlines",
    "REPUBLIC AIRWAYS INC":                            "Republic Airways",
    "AIR WISCONSIN AIRLINES LLC":                      "Air Wisconsin",
    "ENVOY AIR INC":                                   "Envoy Air",
    "MESA AIR GROUP INC":                              "Mesa Airlines",
    "FRONTIER AIRLINES INC":                           "Frontier Airlines",
    "SPIRIT AIRLINES INC":                             "Spirit Airlines",
    "ALLEGIANT AIR LLC":                               "Allegiant Air",
    "SUN COUNTRY AIRLINES":                            "Sun Country Airlines",
    "HAWAIIAN AIRLINES INC":                           "Hawaiian Airlines",
    "FEDERAL EXPRESS CORP":                            "FedEx",
    "FEDERAL EXPRESS CORPORATION":                     "FedEx",
    "UNITED PARCEL SERVICE CO":                        "UPS Airlines",
    "AMAZON.COM SERVICES LLC":                         "Amazon Air",
    "AIR METHODS LLC":                                 "Air Methods",
    "AIR EVAC EMS INC":                                "Air Evac EMS",
    "PHI HEALTH LLC":                                  "PHI Health",
    "GUARDIAN FLIGHT LLC":                             "Guardian Flight",
    "MED-TRANS CORP":                                  "Med-Trans",
    "METRO AVIATION INC":                              "Metro Aviation",
    "NETJETS SALES INC":                               "NetJets",
    "FLEXJET LLC":                                     "Flexjet",
    "WHEELS UP PARTNERS LLC":                          "Wheels Up",
    "CIVIL AIR PATROL":                                "Civil Air Patrol",
    "CIVIL AIR PATROL INC":                            "Civil Air Patrol",
    "EMBRY-RIDDLE AERONAUTICAL UNIVERSITY INC":        "Embry-Riddle Aeronautical University",
    # ── Canada ─────────────────────────────────────────────────────────────────
    "AIR CANADA":                                      "Air Canada",
    "Air Canada":                                      "Air Canada",
    "WESTJET":                                         "WestJet",
    "Westjet":                                         "WestJet",
    "JAZZ AVIATION LP":                                "Jazz Aviation",
    "Jazz Aviation LP":                                "Jazz Aviation",
    "CANADIAN HELICOPTERS LIMITED":                    "Canadian Helicopters",
    "Canadian Helicopters Limited - Hlicoptres Canadiens Limite": "Canadian Helicopters",
    "ROYAL CANADIAN AIR FORCE":                        "Royal Canadian Air Force",
    "PORTER AIRLINES INC":                             "Porter Airlines",
    "AIR TRANSAT AT INC":                              "Air Transat",
    "SUNWING AIRLINES INC":                            "Sunwing Airlines",
    "FLAIR AIRLINES LTD":                              "Flair Airlines",
    # ── United Kingdom ─────────────────────────────────────────────────────────
    "BRITISH AIRWAYS PLC":                             "British Airways",
    "EASYJET AIRLINE CO LTD":                          "easyJet",
    "EASYJET UK LIMITED":                              "easyJet",
    "VIRGIN ATLANTIC AIRWAYS LTD":                     "Virgin Atlantic",
    "TUI AIRWAYS LIMITED":                             "TUI Airways",
    "WIZZ AIR UK LTD":                                 "Wizz Air",
    "LOGANAIR LIMITED":                                "Loganair",
    "FLYBE LIMITED":                                   "Flybe",
    "JET2.COM LIMITED":                                "Jet2",
    # ── Ireland ────────────────────────────────────────────────────────────────
    "RYANAIR DAC":                                     "Ryanair",
    "RYANAIR LIMITED":                                 "Ryanair",
    "AER LINGUS LIMITED":                              "Aer Lingus",
    # ── Europe ─────────────────────────────────────────────────────────────────
    "LUFTHANSA TECHNIK AG":                            "Lufthansa Technik",
    "DEUTSCHE LUFTHANSA AG":                           "Lufthansa",
    "AIR FRANCE":                                      "Air France",
    "KLM ROYAL DUTCH AIRLINES":                        "KLM",
    "SWISS INTERNATIONAL AIR LINES LTD":               "Swiss",
    "AUSTRIAN AIRLINES AG":                            "Austrian Airlines",
    "BRUSSELS AIRLINES NV/SA":                         "Brussels Airlines",
    "SCANDINAVIAN AIRLINES SYSTEM":                    "SAS",
    "NORWEGIAN AIR SHUTTLE ASA":                       "Norwegian",
    "NORWEGIAN AIR SWEDEN AB":                         "Norwegian",
    "WIZZ AIR HUNGARY LTD":                            "Wizz Air",
    "WIZZ AIR MALTA LIMITED":                          "Wizz Air",
    "VUELING AIRLINES SA":                             "Vueling",
    "IBERIA LINEAS AEREAS DE ESPANA SA":               "Iberia",
    "IBERIA EXPRESS SA":                               "Iberia Express",
    "TAP AIR PORTUGAL":                                "TAP Air Portugal",
    "TURKISH AIRLINES INC":                            "Turkish Airlines",
    "TURK HAVA YOLLARI AO":                            "Turkish Airlines",
    "FINNAIR OYJ":                                     "Finnair",
    "LOT POLISH AIRLINES SA":                          "LOT Polish Airlines",
    "CZECH AIRLINES AS":                               "Czech Airlines",
    # ── Middle East ────────────────────────────────────────────────────────────
    "EMIRATES":                                        "Emirates",
    "ETIHAD AIRWAYS":                                  "Etihad Airways",
    "QATAR AIRWAYS":                                   "Qatar Airways",
    "FLYDUBAI":                                        "flydubai",
    "AIR ARABIA PJSC":                                 "Air Arabia",
    # ── Asia ───────────────────────────────────────────────────────────────────
    "SINGAPORE AIRLINES LIMITED":                      "Singapore Airlines",
    "SCOOT TIGERAIR PTE LTD":                          "Scoot",
    "CATHAY PACIFIC AIRWAYS LIMITED":                  "Cathay Pacific",
    "HONG KONG AIRLINES LIMITED":                      "Hong Kong Airlines",
    "AIR ASIA X SDN BHD":                              "AirAsia X",
    "AIRASIA BERHAD":                                  "AirAsia",
    "MALAYSIA AIRLINES BERHAD":                        "Malaysia Airlines",
    "THAI AIRWAYS INTERNATIONAL PCL":                  "Thai Airways",
    "THAI LION AIR CO LTD":                            "Thai Lion Air",
    "VIETNAM AIRLINES JSC":                            "Vietnam Airlines",
    "GARUDA INDONESIA":                                "Garuda Indonesia",
    "BATIK AIR":                                       "Batik Air",
    "LION AIR":                                        "Lion Air",
    "CEBU AIR INC":                                    "Cebu Pacific",
    "PHILIPPINE AIRLINES INC":                         "Philippine Airlines",
    "ALL NIPPON AIRWAYS CO LTD":                       "ANA",
    "JAPAN AIRLINES CO LTD":                           "Japan Airlines",
    "AIR JAPAN CO LTD":                                "Air Japan",
    "KOREAN AIR LINES CO LTD":                         "Korean Air",
    "ASIANA AIRLINES INC":                             "Asiana Airlines",
    "JEJU AIR CO LTD":                                 "Jeju Air",
    "EVA AIRWAYS CORPORATION":                         "EVA Air",
    "CHINA AIRLINES LTD":                              "China Airlines",
    "CHINA EASTERN AIRLINES CO LTD":                   "China Eastern",
    "CHINA SOUTHERN AIRLINES CO LTD":                  "China Southern",
    "AIR CHINA LIMITED":                               "Air China",
    # ── Africa ─────────────────────────────────────────────────────────────────
    "SOUTH AFRICAN AIRWAYS":                           "South African Airways",
    "ETHIOPIAN AIRLINES":                              "Ethiopian Airlines",
    "KENYA AIRWAYS LTD":                               "Kenya Airways",
    "EGYPTAIR":                                        "EgyptAir",
    # ── Latin America ──────────────────────────────────────────────────────────
    "LATAM AIRLINES GROUP SA":                         "LATAM Airlines",
    "LATAM AIRLINES BRASIL SA":                        "LATAM Brasil",
    "AVIANCA SA":                                      "Avianca",
    "GOL LINHAS AEREAS INTELIGENTES SA":               "GOL",
    "COPA AIRLINES SA":                                "Copa Airlines",
    "VOLARIS SA DE CV":                                "Volaris",
    "AEROMEXICO SA DE CV":                             "Aeroméxico",
}

# Words to keep in uppercase when title-casing unmapped operator names.
_KEEP_UPPER = {
    "LLC", "INC", "LTD", "LP", "NA", "USA", "UK", "AB", "AS", "SA",
    "NV", "BV", "AG", "KG", "SE", "PTY", "PLC", "DAC", "JSC", "PCL",
}


def to_display_name(raw: str) -> str:
    """Return a curated brand name, or a title-cased fallback for unmapped operators."""
    if raw in BRAND_MAP:
        return BRAND_MAP[raw]
    words = []
    for w in raw.split():
        words.append(w if w in _KEEP_UPPER else w.capitalize())
    return " ".join(words)


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
    active          INTEGER NOT NULL DEFAULT 1,
    brand           TEXT NOT NULL DEFAULT ''
);
"""

CREATE_INDEX_ICAO24 = "CREATE UNIQUE INDEX idx_icao24 ON aircraft (icao24);"
CREATE_INDEX_REG    = "CREATE INDEX idx_registration ON aircraft (registration);"
CREATE_INDEX_BRAND  = "CREATE INDEX idx_brand ON aircraft (brand);"


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
        brand        = to_display_name(operator_) if operator_ else ""

        cur.execute(
            "INSERT INTO aircraft VALUES (?,?,?,?,?,?,?)",
            (icao24, registration, typecode, type_long, operator_, 1, brand),
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
            op = prev["operator"]
            cur.execute(
                "INSERT OR IGNORE INTO aircraft VALUES (?,?,?,?,?,?,?)",
                (
                    icao24,
                    prev["registration"],
                    prev["typecode"],
                    prev["model"],
                    op,
                    0,  # inactive
                    to_display_name(op) if op else "",
                ),
            )
            retired += 1
        if retired:
            print(f"  … {retired:,} retired aircraft preserved (active=0)")

    conn.commit()

    print("Building indexes …")
    cur.executescript(CREATE_INDEX_ICAO24 + "\n" + CREATE_INDEX_REG + "\n" + CREATE_INDEX_BRAND)
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
