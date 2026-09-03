-- Operational tables shared by every product. Every ingest job and every dbt run writes here.
create schema if not exists ops;

create table if not exists ops.sync_runs (
  run_id       bigint generated always as identity primary key,
  job          text        not null,          -- e.g. 'sui.collect_pools', 'dbt.build'
  product      text,                          -- 'sui' | 'morpho' | 'treasuries' | null for shared
  started_at   timestamptz not null default now(),
  finished_at  timestamptz,
  status       text        not null default 'running',  -- running | ok | error | skipped
  rows_written integer,
  error        text,
  notes        jsonb       not null default '{}'::jsonb,
  runner       text                           -- 'github-actions' | 'cloudflare' | 'local'
);
create index if not exists sync_runs_job_started_idx on ops.sync_runs (job, started_at desc);

create table if not exists ops.sync_cursors (
  job        text primary key,
  cursor     text,                            -- opaque: a checkpoint, a block, a page token, a timestamp
  updated_at timestamptz not null default now(),
  notes      jsonb not null default '{}'::jsonb
);

-- One row per source per product: what we expect and when we last saw it. Feeds /api/data-health.
create table if not exists ops.source_freshness (
  product        text not null,
  source_id      text not null,
  expected_hours numeric not null,
  last_seen_at   timestamptz,
  last_status    text,
  updated_at     timestamptz not null default now(),
  primary key (product, source_id)
);

-- Read-only role for apps, API and MCP. Password is set out of band:
--   alter role datum_reader with password '...';
do $$ begin
  if not exists (select 1 from pg_roles where rolname = 'datum_reader') then
    create role datum_reader nologin;
  end if;
end $$;
