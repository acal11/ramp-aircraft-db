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

## Running locally

```bash
python3 generate_aircraft_db.py                               # commercial fleet only
python3 generate_aircraft_db.py --all                         # all aircraft
python3 generate_aircraft_db.py --previous-db old.db          # merge retired aircraft
python3 generate_aircraft_db.py --output /tmp/aircraft.db     # custom output path
```
