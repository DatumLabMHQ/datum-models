-- Raw layer for Morpho: hourly snapshots of every market and vault on every chain from the
-- Morpho GraphQL API, the curator registry, and DefiLlama TVL for Morpho and its comparators.
-- Append-only; every row carries run_id, source_id and fetched_at.
create schema if not exists morpho;
create table if not exists morpho.raw_market_snapshots (
  snapshot_id bigint generated always as identity primary key,
  run_id bigint not null, source_id text not null default 'morpho_graphql', fetched_at timestamptz not null,
  chain_id integer not null, market_id text not null, listed boolean,
  collateral_symbol text, collateral_address text, loan_symbol text, loan_address text, loan_decimals integer,
  lltv numeric, supply_assets_usd double precision, borrow_assets_usd double precision, collateral_assets_usd double precision,
  liquidity_assets_usd double precision, utilization double precision, supply_apy double precision, borrow_apy double precision,
  net_supply_apy double precision, fee double precision, bad_debt_usd double precision, state_timestamp timestamptz,
  payload jsonb
);
create index if not exists raw_market_snapshots_market_ts on morpho.raw_market_snapshots (chain_id, market_id, fetched_at desc);
create index if not exists raw_market_snapshots_fetched on morpho.raw_market_snapshots (fetched_at);
create table if not exists morpho.raw_vault_snapshots (
  snapshot_id bigint generated always as identity primary key,
  run_id bigint not null, source_id text not null default 'morpho_graphql', fetched_at timestamptz not null,
  chain_id integer not null, vault_address text not null, name text, symbol text, listed boolean,
  asset_symbol text, asset_address text, total_assets_usd double precision, apy double precision, net_apy double precision,
  net_apy_excl_rewards double precision, fee double precision, share_price_usd double precision, curator_address text,
  curator_ids text[], curator_names text[], state_timestamp timestamptz, payload jsonb
);
create index if not exists raw_vault_snapshots_vault_ts on morpho.raw_vault_snapshots (chain_id, vault_address, fetched_at desc);
create index if not exists raw_vault_snapshots_fetched on morpho.raw_vault_snapshots (fetched_at);
create table if not exists morpho.raw_curators (
  curator_id text primary key, name text, verified boolean, addresses jsonb, aum double precision, fetched_at timestamptz not null, run_id bigint
);
create table if not exists morpho.raw_defillama_tvl (
  slug text not null, chain text not null, day date not null, tvl_usd double precision, borrowed_usd double precision,
  fetched_at timestamptz not null, run_id bigint not null,
  primary key (slug, chain, day, run_id)
);
create index if not exists morpho_raw_defillama_latest on morpho.raw_defillama_tvl (slug, chain, day, fetched_at desc);
grant usage on schema morpho to datum_reader;
alter default privileges in schema morpho grant select on tables to datum_reader;
grant select on all tables in schema morpho to datum_reader;
