{{ config(materialized='table') }}
-- One row per UTC day: last observed totals of the day (the worker snapshots every 5 minutes).
with ranked as (
  select *, row_number() over (partition by day order by ts desc) as rn from {{ ref('stg_rwa__market_history') }}
)
select day, ts as as_of, rwa_aum as rwa_aum_usd, stablecoin_aum as stablecoin_aum_usd, total_aum as total_aum_usd,
       horizon_supplied_usd, holders, active_addresses, actions, issuers
from ranked where rn = 1
