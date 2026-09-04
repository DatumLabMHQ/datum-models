{{ config(materialized='incremental', unique_key=['day', 'chain_id', 'market_id'], incremental_strategy='delete+insert', on_schema_change='append_new_columns') }}
-- Incremental: raw rows older than 60 days move to R2 (tiering), so this mart keeps its own history and
-- only recomputes the last 3 days each run. A full refresh rebuilds only what is still hot in Neon.
-- One row per market per UTC day: the last snapshot of the day. Unlisted markets are kept but flagged;
-- readers filter listed = true for headline numbers (the API also returns dust and broken markets).
with ranked as (
  select *, row_number() over (partition by chain_id, market_id, day order by fetched_at desc) as rn from {{ ref('stg_morpho__market_snapshots') }}
  {% if is_incremental() %} where day >= current_date - 3 {% endif %}
)
select day, fetched_at as as_of, chain_id, market_id, listed, collateral_symbol, collateral_address, loan_symbol, loan_address, lltv,
       supply_assets_usd, borrow_assets_usd, collateral_assets_usd, liquidity_assets_usd, utilization, supply_apy, borrow_apy, net_supply_apy, fee_pct, bad_debt_usd
from ranked where rn = 1
