{{ config(materialized='incremental', unique_key=['reserve', 'day'], incremental_strategy='delete+insert', on_schema_change='append_new_columns') }}
-- Incremental: raw rows older than 60 days move to R2 (tiering), so this mart keeps its own history and
-- only recomputes the last 3 days each run. A full refresh rebuilds only what is still hot in Neon.
-- One row per Horizon reserve per UTC day, last value of the day.
with ranked as (
  select *, row_number() over (partition by reserve, day order by ts desc) as rn from {{ ref('stg_rwa__reserve_state_history') }}
  {% if is_incremental() %} where day >= current_date - 3 {% endif %}
)
select day, ts as as_of, reserve, symbol, supplied_usd, borrowed_usd, supply_apy, borrow_apy, utilization, ltv, liquidation_threshold, oracle_price, nav
from ranked where rn = 1
