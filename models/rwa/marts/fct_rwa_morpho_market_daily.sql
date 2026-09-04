{{ config(materialized='incremental', unique_key=['market_id', 'day'], incremental_strategy='delete+insert', on_schema_change='append_new_columns') }}
-- Incremental: raw rows older than 60 days move to R2 (tiering), so this mart keeps its own history and
-- only recomputes the last 3 days each run. A full refresh rebuilds only what is still hot in Neon.
-- One row per Morpho RWA-collateral market per UTC day, last value of the day.
with ranked as (
  select *, row_number() over (partition by market_id, day order by ts desc) as rn from {{ ref('stg_rwa__morpho_market_history') }}
  {% if is_incremental() %} where day >= current_date - 3 {% endif %}
)
select day, ts as as_of, market_id, collateral_symbol, loan_symbol, asset_class, lltv, collateral_usd, borrow_usd, utilization, borrow_apy
from ranked where rn = 1
