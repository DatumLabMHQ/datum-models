-- Centrifuge raw layer from api.centrifuge.io: hourly pool and token snapshots (with per-chain
-- token instances), recent investor transactions (deduped), and the API's own token history
-- (tokenSnapshots) which backfills supply and price before our first run.
create schema if not exists centrifuge;
create table if not exists centrifuge.raw_pool_snapshots (
  snapshot_id bigint generated always as identity primary key,
  run_id bigint not null, source_id text not null default 'centrifuge_api', fetched_at timestamptz not null,
  pool_id text not null, name text, is_active boolean, currency text, decimals integer, metadata text, token_count integer, payload jsonb
);
create index if not exists centrifuge_pool_snapshots_fetched on centrifuge.raw_pool_snapshots (fetched_at);
create table if not exists centrifuge.raw_token_snapshots (
  snapshot_id bigint generated always as identity primary key,
  run_id bigint not null, source_id text not null default 'centrifuge_api', fetched_at timestamptz not null,
  pool_id text not null, token_id text not null, symbol text, name text, decimals integer, is_active boolean,
  total_issuance numeric, token_price numeric, instances jsonb, payload jsonb
);
create index if not exists centrifuge_token_snapshots_key on centrifuge.raw_token_snapshots (token_id, fetched_at desc);
create index if not exists centrifuge_token_snapshots_fetched on centrifuge.raw_token_snapshots (fetched_at);
create table if not exists centrifuge.raw_token_history (
  token_id text not null, ts timestamptz not null, total_issuance numeric, token_price numeric,
  fetched_at timestamptz not null, run_id bigint not null,
  primary key (token_id, ts)
);
create table if not exists centrifuge.raw_investor_transactions (
  event_key text primary key, run_id bigint not null, source_id text not null default 'centrifuge_api', fetched_at timestamptz not null,
  tx_hash text, type text, account text, pool_id text, token_id text, centrifuge_id text, chain_id integer, chain_name text,
  currency_amount numeric, token_amount numeric, token_price numeric, created_at timestamptz, payload jsonb
);
create index if not exists centrifuge_investor_tx_created on centrifuge.raw_investor_transactions (created_at);
grant usage on schema centrifuge to datum_reader;
alter default privileges in schema centrifuge grant select on tables to datum_reader;
grant select on all tables in schema centrifuge to datum_reader;
