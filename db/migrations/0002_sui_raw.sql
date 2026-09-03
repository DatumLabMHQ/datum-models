-- Raw layer for the Sui lending product. Exactly what the adapters return, plus provenance.
create schema if not exists sui;

create table if not exists sui.raw_pool_snapshots (
  id                    bigint generated always as identity primary key,
  run_id                bigint references ops.sync_runs(run_id),
  source_id             text        not null,      -- sources.yaml id, e.g. 'navi_api'
  fetched_at            timestamptz not null,
  protocol              text        not null,
  symbol                text        not null,
  coin_type             text,
  decimals              integer,
  total_supply          numeric,                    -- token units
  total_supply_usd      double precision,
  total_borrows         numeric,
  total_borrows_usd     double precision,
  available_liquidity   numeric,
  available_liquidity_usd double precision,
  supply_apy            double precision,           -- percent as number, base only
  borrow_apy            double precision,
  incentive_supply_apy  double precision,
  incentive_borrow_apy  double precision,
  utilization           double precision,           -- percent 0..100
  ltv                   double precision,           -- percent 0..100
  liquidation_threshold double precision,           -- percent 0..100
  price_usd             double precision,
  irm                   jsonb,
  payload               jsonb       not null        -- the normalised pool as returned by the adapter
);
create index if not exists raw_pool_snapshots_proto_sym_ts on sui.raw_pool_snapshots (protocol, symbol, fetched_at desc);

create table if not exists sui.raw_liquidation_events (
  event_id          text primary key,               -- txDigest:eventSeq
  run_id            bigint references ops.sync_runs(run_id),
  source_id         text        not null,
  fetched_at        timestamptz not null,
  protocol          text        not null,
  tx_digest         text        not null,
  ts                timestamptz not null,
  liquidator        text,
  borrower          text,
  collateral_asset  text,
  collateral_amount double precision,
  collateral_price  double precision,
  collateral_usd    double precision,
  debt_asset        text,
  debt_amount       double precision,
  debt_price        double precision,
  debt_usd          double precision,
  treasury_amount   double precision,
  gas_used_mist     numeric,
  gas_usd           double precision,
  checkpoint        bigint,
  payload           jsonb       not null
);
create index if not exists raw_liq_proto_ts on sui.raw_liquidation_events (protocol, ts desc);

create table if not exists sui.raw_defillama_tvl (
  protocol   text        not null,
  slug       text        not null,
  day        date        not null,
  tvl_usd    double precision,
  fetched_at timestamptz not null,
  run_id     bigint references ops.sync_runs(run_id),
  primary key (protocol, day)
);

grant usage on schema sui to datum_reader;
grant usage on schema ops to datum_reader;
alter default privileges in schema sui grant select on tables to datum_reader;
alter default privileges in schema ops grant select on tables to datum_reader;
grant select on all tables in schema sui to datum_reader;
grant select on all tables in schema ops to datum_reader;
