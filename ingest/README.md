# Ingest jobs

Each job fills one or more `raw_*` tables. Contract:

1. Open a run: insert into `ops.sync_runs` with status `running`; keep the `run_id`.
2. Read the cursor for the job from `ops.sync_cursors` (may be null on first run).
3. Fetch from the source, append rows with `run_id`, `source_id`, `fetched_at`.
4. Advance the cursor only after the rows are committed.
5. Close the run with `ok` and `rows_written`, or `error` and the message. Never leave it `running`.
6. Update `ops.source_freshness` for each source touched.

Backfill and live are separate jobs even when they write the same table.

The first Sui job runs from the SuiLending repository's adapters (the code that already knows
every protocol) and writes here; see `ingest/sui/README.md`.
