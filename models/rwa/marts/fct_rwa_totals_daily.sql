{{ config(materialized='incremental', unique_key=['day'], incremental_strategy='delete+insert', on_schema_change='append_new_columns') }}
-- Incremental: raw rows older than 60 days move to R2 (tiering), so this mart keeps its own history and
-- only recomputes the last 3 days each run. A full refresh rebuilds only what is still hot in Neon.
-- One row per UTC day: last observed totals of the day (the worker snapshots every 5 minutes).
with ranked as (
  select *, row_number() over (partition by day order by ts desc) as rn from {{ ref('stg_rwa__market_history') }}
  {% if is_incremental() %} where day >= current_date - 3 {% endif %}
)
select day, ts as as_of, rwa_aum as rwa_aum_usd, stablecoin_aum as stablecoin_aum_usd, total_aum as total_aum_usd,
       horizon_supplied_usd, holders, active_addresses, actions, issuers
from ranked where rn = 1
