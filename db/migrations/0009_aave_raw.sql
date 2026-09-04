-- Aave raw layer: hourly reserve snapshots from the Aave v3 API (20 chains, every market incl.
-- Lido/EtherFi/Horizon) and the Aave v4 API (Ethereum hub). Append-only with run provenance.
create schema if not exists aave;
create table if not exists aave.raw_reserve_snapshots (
  snapshot_id bigint generated always as identity primary key,
  run_id bigint not null, source_id text not null, fetched_at timestamptz not null,
  version text not null, chain_id integer not null, chain_name text, market_name text, market_address text,
  underlying_address text, symbol text, decimals integer, price_usd double precision,
  supply_amount double precision, supply_usd double precision, supply_apy double precision, liquidation_threshold double precision,
  borrow_amount double precision, borrow_usd double precision, borrow_apy double precision, utilization double precision,
  payload jsonb
);
create index if not exists aave_reserve_snapshots_key on aave.raw_reserve_snapshots (version, chain_id, market_address, underlying_address, fetched_at desc);
create index if not exists aave_reserve_snapshots_fetched on aave.raw_reserve_snapshots (fetched_at);
create table if not exists aave.raw_market_snapshots (
  snapshot_id bigint generated always as identity primary key,
  run_id bigint not null, source_id text not null, fetched_at timestamptz not null,
  version text not null, chain_id integer not null, chain_name text, market_name text, market_address text,
  total_market_size_usd double precision, reserve_count integer
);
create index if not exists aave_market_snapshots_fetched on aave.raw_market_snapshots (fetched_at);
grant usage on schema aave to datum_reader;
alter default privileges in schema aave grant select on tables to datum_reader;
grant select on all tables in schema aave to datum_reader;
