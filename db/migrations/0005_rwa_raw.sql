-- Raw layer for the RWA terminal. Mirrors the history tables the Cloudflare worker writes
-- every 5 minutes into the legacy Neon database, plus the small dimension tables. Rows are
-- copied read-only from the legacy database (scripts/import_legacy_rwa.py) until the worker
-- is pointed at the platform (migration plan, week 9). Append-only; provenance on every row.
create schema if not exists rwa;

create table if not exists rwa.raw_market_history (
  ts timestamptz not null primary key,
  rwa_aum double precision, stablecoin_aum double precision, total_aum double precision,
  horizon_supplied_usd double precision, holders integer, active_addresses integer, actions integer, issuers integer,
  source_id text not null default 'legacy_neon', run_id bigint, imported_at timestamptz not null default now()
);
create table if not exists rwa.raw_reserve_state_history (
  ts timestamptz not null, reserve text not null, symbol text,
  supplied_usd double precision, supply_apy_pct double precision, ltv_pct double precision, liq_threshold_pct double precision,
  oracle_price double precision, nav double precision, borrowed_usd double precision, utilization_pct double precision, borrow_apy_pct double precision,
  source_id text not null default 'legacy_neon', run_id bigint, imported_at timestamptz not null default now(),
  primary key (ts, reserve)
);
create table if not exists rwa.raw_horizon_holder_history (
  ts timestamptz not null, reserve text not null, symbol text, asset_class text, holders integer,
  source_id text not null default 'legacy_neon', run_id bigint, imported_at timestamptz not null default now(),
  primary key (ts, reserve)
);
create table if not exists rwa.raw_morpho_market_history (
  ts timestamptz not null, market_id text not null, collateral_symbol text, loan_symbol text, asset_class text,
  lltv double precision, collateral_usd double precision, borrow_usd double precision, utilization double precision, borrow_apy double precision,
  source_id text not null default 'legacy_neon', run_id bigint, imported_at timestamptz not null default now(),
  primary key (ts, market_id)
);
create table if not exists rwa.raw_morpho_risk_history (
  ts timestamptz not null primary key,
  total_collateral_usd double precision, total_borrow_usd double precision, avg_lltv double precision, market_count integer, borrowers integer,
  min_health_factor double precision, hf_at_risk integer, hf_tight integer, hf_moderate integer, hf_safe integer,
  source_id text not null default 'legacy_neon', run_id bigint, imported_at timestamptz not null default now()
);
create table if not exists rwa.raw_asset_aum_history (
  asset_id bigint not null, ts timestamptz not null, aum numeric, source text,
  source_id text not null default 'legacy_neon', run_id bigint, imported_at timestamptz not null default now(),
  primary key (asset_id, ts)
);
create table if not exists rwa.raw_asset_nav_history (
  asset_id bigint not null, ts timestamptz not null, nav numeric, source text, updated_at_onchain timestamptz, round_id numeric, is_stale boolean,
  source_id text not null default 'legacy_neon', run_id bigint, imported_at timestamptz not null default now(),
  primary key (asset_id, ts)
);
create table if not exists rwa.raw_token_supply_history (
  token_id bigint not null, ts timestamptz not null, total_supply numeric, circulating numeric,
  source_id text not null default 'legacy_neon', run_id bigint, imported_at timestamptz not null default now(),
  primary key (token_id, ts)
);
create table if not exists rwa.raw_token_top_holders (
  address text not null, fetched_at timestamptz not null, symbol text, holders jsonb,
  source_id text not null default 'legacy_neon', run_id bigint, imported_at timestamptz not null default now(),
  primary key (address, fetched_at)
);
-- Dimensions: small, refreshed in full on every import (latest copy wins).
create table if not exists rwa.raw_asset (
  asset_id bigint primary key, name text, display_ticker text, issuer_id bigint, investment_manager_id bigint, platform_id bigint,
  transfer_agent_id bigint, isin text, redemption_terms jsonb, settlement_cycle text, rating text, created_at timestamptz, updated_at timestamptz,
  imported_at timestamptz not null default now()
);
create table if not exists rwa.raw_token (
  token_id bigint primary key, asset_id bigint, chain_id integer, contract_address text, token_standard text, decimals smallint, symbol text,
  distributed_or_represented text, is_wrapper boolean, wraps_token_id bigint, oracle_provider_id bigint, nav_aggregator_addr text,
  nav_heartbeat_secs integer, created_at timestamptz, imported_at timestamptz not null default now()
);
create table if not exists rwa.raw_issuer (
  issuer_id bigint primary key, name text, legal_name text, type text, website text, data_source_url text, notes text,
  created_at timestamptz, updated_at timestamptz, imported_at timestamptz not null default now()
);
create index if not exists raw_reserve_state_history_reserve_ts on rwa.raw_reserve_state_history (reserve, ts desc);
create index if not exists raw_morpho_market_history_market_ts on rwa.raw_morpho_market_history (market_id, ts desc);
create index if not exists raw_asset_aum_history_asset_ts on rwa.raw_asset_aum_history (asset_id, ts desc);
grant usage on schema rwa to datum_reader;
alter default privileges in schema rwa grant select on tables to datum_reader;
grant select on all tables in schema rwa to datum_reader;
