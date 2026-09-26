-- Raw layer for the RWA terminal's own ingest (scripts/ingest_rwa.py), beside the legacy import:
--   raw_horizon_emode    Aave Horizon e-mode categories read from the pool contract: one row per category and reserve,
--                        role collateral or borrowable. Borrowing on Horizon happens in e-mode, so these are the
--                        parameters borrowers actually get; the base LTVs in raw_reserve_state_history are the fallback.
--   raw_token_supply     totalSupply of each tokenized asset on every chain in seeds/rwa_token_registry.csv.
--   raw_euler_vaults     Euler v2 vaults holding RWA tokens, and the borrowable vaults of RWA clusters (Euler v3 API).
--   raw_euler_collaterals one row per borrowable vault and accepted collateral vault: LTVs and debt the collateral backs.
-- Append-only; every row carries run_id, source_id and fetched_at.
create table if not exists rwa.raw_horizon_emode (
  row_id bigint generated always as identity primary key,
  run_id bigint not null, source_id text not null default 'horizon_rpc', fetched_at timestamptz not null,
  chain_id integer not null, pool text not null, category_id integer not null, label text,
  ltv_pct double precision, liquidation_threshold_pct double precision, liquidation_bonus_pct double precision,
  reserve text not null, symbol text, role text not null
);
create index if not exists raw_horizon_emode_fetched on rwa.raw_horizon_emode (fetched_at);

create table if not exists rwa.raw_token_supply (
  row_id bigint generated always as identity primary key,
  run_id bigint not null, source_id text not null default 'chain_rpc', fetched_at timestamptz not null,
  ticker text not null, chain text not null, address text not null, total_supply numeric
);
create index if not exists raw_token_supply_fetched on rwa.raw_token_supply (fetched_at);

create table if not exists rwa.raw_euler_vaults (
  row_id bigint generated always as identity primary key,
  run_id bigint not null, source_id text not null default 'euler_v3_api', fetched_at timestamptz not null,
  chain_id integer not null, vault text not null, asset_address text, asset_symbol text, asset_decimals integer,
  total_assets numeric, total_borrows numeric, supply_usd double precision, borrow_usd double precision,
  utilization double precision, supply_apy double precision, borrow_apy double precision,
  cluster text, curator text, role text, discovered boolean not null default false, payload jsonb
);
create index if not exists raw_euler_vaults_fetched on rwa.raw_euler_vaults (fetched_at);

create table if not exists rwa.raw_euler_collaterals (
  row_id bigint generated always as identity primary key,
  run_id bigint not null, source_id text not null default 'euler_v3_api', fetched_at timestamptz not null,
  chain_id integer not null, borrow_vault text not null, collateral_vault text not null,
  borrow_ltv double precision, liquidation_ltv double precision, debt_backed_usd double precision
);
create index if not exists raw_euler_collaterals_fetched on rwa.raw_euler_collaterals (fetched_at);

grant select on rwa.raw_horizon_emode, rwa.raw_token_supply, rwa.raw_euler_vaults, rwa.raw_euler_collaterals to datum_reader;
