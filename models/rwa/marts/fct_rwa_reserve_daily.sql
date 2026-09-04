{{ config(materialized='table') }}
-- One row per Horizon reserve per UTC day, last value of the day.
with ranked as (
  select *, row_number() over (partition by reserve, day order by ts desc) as rn from {{ ref('stg_rwa__reserve_state_history') }}
)
select day, ts as as_of, reserve, symbol, supplied_usd, borrowed_usd, supply_apy, borrow_apy, utilization, ltv, liquidation_threshold, oracle_price, nav
from ranked where rn = 1
