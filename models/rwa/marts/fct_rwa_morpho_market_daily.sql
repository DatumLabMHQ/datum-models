{{ config(materialized='table') }}
-- One row per Morpho RWA-collateral market per UTC day, last value of the day.
with ranked as (
  select *, row_number() over (partition by market_id, day order by ts desc) as rn from {{ ref('stg_rwa__morpho_market_history') }}
)
select day, ts as as_of, market_id, collateral_symbol, loan_symbol, asset_class, lltv, collateral_usd, borrow_usd, utilization, borrow_apy
from ranked where rn = 1
