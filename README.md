# ramp-aircraft-db

Public pipeline for the Ramp iOS planespotter app's aircraft database.

Runs weekly via GitHub Actions to download the latest [tar1090-db](https://github.com/wiedehopf/tar1090-db) data, generate a SQLite database, and publish it as a GitHub Release alongside a `manifest.json` that the app checks on launch.

## Releases

Each release contains:
- `aircraft.db` — SQLite database of every aircraft with a registration and type code (~58 MB)
- `manifest.json` — version manifest checked by the app at launch

## Data source

[wiedehopf/tar1090-db](https://github.com/wiedehopf/tar1090-db) (ODC-BY licence)

## Retired aircraft

Aircraft removed from the source data are preserved in the database with `active = 0`. This ensures historical sightings in the app retain their type and operator display data, while retired registrations are excluded from autocomplete suggestions and fleet collection views.

There is no operator filter (removed 2026-10-17): every row with a registration and type code is included. Filtering on the operator left the database ~95% US, because that column comes from ADS-B Exchange and is populated almost only for the US register.

An aircraft the source still lists is never marked retired. The operator column comes from ADS-B Exchange alone (via tar1090-db) and has dropped whole registers at once (Australia, between the 2026-10-05 and 2026-10-12 builds). When an aircraft loses its operator but keeps its registration, it keeps the previous build's operator. A manual run can also name a `seed_tag`, an older release used only to fill operators that nothing newer has. It contributes no rows of its own. `aircraft-db-20260803` is the last release with Canadian operators.

## Release gate

`check_db_shrink.py` runs before signing and fails the workflow if the new build has lost more than 5% of its active rows, more than 20% of any registration prefix with 500+ rows, or falls below the absolute floors for the app's home markets (total, `N`, `VH`). A failed gate publishes nothing.

To recover from a bad release, run the workflow manually with `previous_tag` set to the last good release (e.g. `aircraft-db-20261005`). Use `allow_shrink` only for a confirmed, genuine upstream change.

## Running locally

```bash
python3 generate_aircraft_db.py                               # every aircraft with reg + type
python3 generate_aircraft_db.py --previous-db old.db          # merge retired aircraft, carry operators
python3 generate_aircraft_db.py --seed-db older.db            # fill missing operators from an older build
python3 generate_aircraft_db.py --output /tmp/aircraft.db     # custom output path
python3 check_db_shrink.py new.db --previous old.db           # release gate
```
