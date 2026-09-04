-- Hot/cold tiering receipts. Neon keeps recent raw data; Cloudflare R2 keeps all history as
-- Parquet. A partition is deleted from Neon only after its receipt has verified_at set.
create table if not exists ops.cold_partitions (
  product       text not null,
  table_name    text not null,           -- schema-qualified, e.g. sui.raw_liquidation_events
  day           date not null,
  row_count     bigint not null,
  checksum      text not null,           -- md5 over sorted row hashes, computed in Postgres and re-computed from the Parquet
  object_key    text not null,           -- r2 key: <product>/<table>/day=YYYY-MM-DD/part-<n>.parquet
  bytes         bigint,
  exported_at   timestamptz not null default now(),
  verified_at   timestamptz,
  deleted_from_hot_at timestamptz,
  primary key (table_name, day)
);
create index if not exists cold_partitions_pending on ops.cold_partitions (table_name) where deleted_from_hot_at is null;

-- Legacy imports: one receipt per source table copied from an existing product database.
create table if not exists ops.legacy_imports (
  import_id     bigint generated always as identity primary key,
  product       text not null,
  source_db     text not null,           -- label only, never the connection string
  source_table  text not null,
  target_table  text not null,
  rows_read     bigint not null default 0,
  rows_written  bigint not null default 0,
  min_ts        timestamptz,
  max_ts        timestamptz,
  started_at    timestamptz not null default now(),
  finished_at   timestamptz,
  status        text not null default 'running',
  error         text
);
