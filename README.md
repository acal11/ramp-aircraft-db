# ramp-aircraft-db

Public pipeline for the Ramp iOS planespotter app's aircraft database.

Runs weekly via GitHub Actions to download the latest [tar1090-db](https://github.com/wiedehopf/tar1090-db) data, generate a filtered SQLite database, and publish it as a GitHub Release alongside a `manifest.json` that the app checks on launch.

## Releases

Each release contains:
- `aircraft.db` — filtered SQLite database (~30 MB)
- `manifest.json` — version manifest checked by the app at launch

## Data source

[wiedehopf/tar1090-db](https://github.com/wiedehopf/tar1090-db) (ODC-BY licence)

## Retired aircraft

Aircraft removed from the source data are preserved in the database with `active = 0`. This ensures historical sightings in the app retain their type and operator display data, while retired registrations are excluded from autocomplete suggestions and fleet collection views.

An aircraft the source still lists is never marked retired. The operator column comes from ADS-B Exchange alone (via tar1090-db) and has dropped whole registers at once (Australia, between the 2026-10-05 and 2026-10-12 builds). When an aircraft loses only its operator and keeps its registration, it stays active with the previous build's operator.

## Release gate

`check_db_shrink.py` runs before signing and fails the workflow if the new build has lost more than 5% of its active rows, more than 20% of any registration prefix with 500+ rows, or falls below the absolute floors for the app's home markets (total, `N`, `VH`). A failed gate publishes nothing.

To recover from a bad release, run the workflow manually with `previous_tag` set to the last good release (e.g. `aircraft-db-20261005`). Use `allow_shrink` only for a confirmed, genuine upstream change.

## Running locally

```bash
python3 generate_aircraft_db.py                               # commercial fleet only
python3 generate_aircraft_db.py --all                         # all aircraft
python3 generate_aircraft_db.py --previous-db old.db          # merge retired aircraft
python3 generate_aircraft_db.py --output /tmp/aircraft.db     # custom output path
python3 check_db_shrink.py new.db --previous old.db           # release gate
```
