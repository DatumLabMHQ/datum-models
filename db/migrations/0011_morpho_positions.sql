-- Raw layer for Morpho positions: twice a day, the largest suppliers and borrowers of the largest
-- listed markets with their health factors (scripts/ingest_morpho_positions.py). A sample, not the
-- book: each row carries the market's own totals at the snapshot so readers can say what share it covers.
-- Append-only; every row carries run_id, source_id and fetched_at. Older rows tier to R2 like the other raw tables.
create table if not exists morpho.raw_position_snapshots (
  snapshot_id bigint generated always as identity primary key,
  run_id bigint not null, source_id text not null default 'morpho_graphql', fetched_at timestamptz not null,
  chain_id integer not null, market_id text not null, user_address text not null, side text not null, rank integer not null,
  supply_assets_usd double precision, borrow_assets_usd double precision, collateral_usd double precision,
  supply_shares numeric, borrow_shares numeric, health_factor double precision, price_to_liquidation double precision,
  market_supply_usd double precision, market_borrow_usd double precision, state_timestamp timestamptz
);
create index if not exists raw_position_snapshots_market_ts on morpho.raw_position_snapshots (chain_id, market_id, fetched_at desc);
create index if not exists raw_position_snapshots_fetched on morpho.raw_position_snapshots (fetched_at);
grant select on morpho.raw_position_snapshots to datum_reader;
