# datum-models

The modeled tables behind every Datum Labs product. One Neon database, one schema per product,
three layers per schema, tests on every layer, an hourly run that logs itself.

```
raw   (schema <product>, tables raw_*)   exactly as the source sent it, with provenance columns
stg_  (dbt)                               same grain as raw, units normalised, types fixed
int_  (dbt)                               joins, identity resolution, reusable series
fct_ / dim_ (dbt)                         the curated layer; the only layer products read
```

Definitions live in `datum-context/metrics`. If a model and its metric file disagree, the file
wins and the model is a bug.

## Layout

| Path | What |
|---|---|
| `db/migrations/` | Plain SQL, applied in order by `scripts/migrate.py`. Owns the `ops` schema and every `raw_*` table. dbt never creates raw tables. |
| `models/<product>/` | dbt models: `staging/`, `intermediate/`, `marts/`, each with a `schema.yml` carrying descriptions and tests. |
| `ingest/` | Jobs that fill `raw_*` tables. Each job writes a row to `ops.sync_runs` on start and finish and keeps its position in `ops.sync_cursors`. |
| `scripts/` | Operational helpers: migrate, health, backfill. |
| `.github/workflows/` | The hourly run: migrate, ingest, `dbt build`, health. |

## Running locally

```
set -a; . ~/.config/datum/.env; set +a      # DATABASE_URL and DATABASE_URL_DIRECT
python scripts/migrate.py                    # apply pending migrations (uses DATABASE_URL_DIRECT)
dbt build --profiles-dir . --target dev       # builds and tests every model
```

`profiles.yml` reads the connection from the environment. No credentials in the repo.

## Rules

- Raw is append-only. Corrections happen by re-ingesting into a new run, never by editing rows.
- Every raw row carries `source_id`, `fetched_at` and `run_id`.
- Every fact table has a `day` or `ts` column in UTC and a `method` column when more than one
  way of computing the metric exists.
- A failing test on a mart blocks the mart from updating; the previous rows stay.
- Units follow `datum-context/house/units.md`: USD as doubles, percent as numbers, UTC.
