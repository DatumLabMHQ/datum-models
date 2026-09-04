{{ config(materialized='table') }}
-- One row per market per UTC day: the last snapshot of the day. Unlisted markets are kept but flagged;
-- readers filter listed = true for headline numbers (the API also returns dust and broken markets).
with ranked as (
  select *, row_number() over (partition by chain_id, market_id, day order by fetched_at desc) as rn from {{ ref('stg_morpho__market_snapshots') }}
)
select day, fetched_at as as_of, chain_id, market_id, listed, collateral_symbol, collateral_address, loan_symbol, loan_address, lltv,
       supply_assets_usd, borrow_assets_usd, collateral_assets_usd, liquidity_assets_usd, utilization, supply_apy, borrow_apy, net_supply_apy, fee_pct, bad_debt_usd
from ranked where rn = 1
