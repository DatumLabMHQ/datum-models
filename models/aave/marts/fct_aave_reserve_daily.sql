{{ config(materialized='incremental', unique_key=['day', 'version', 'chain_id', 'market_key', 'underlying_address'], incremental_strategy='delete+insert', on_schema_change='append_new_columns') }}
-- One row per reserve per market per UTC day (last hourly snapshot). v4 reserves have no market address; market_key is 'v4-hub'.
with ranked as (
  select *, row_number() over (partition by version, chain_id, market_key, underlying_address, day order by fetched_at desc) as rn
  from {{ ref('stg_aave__reserve_snapshots') }}
  {% if is_incremental() %} where day >= current_date - 3 {% endif %}
)
select day, fetched_at as as_of, version, chain_id, chain_name, market_name, market_key, market_address, underlying_address, symbol, decimals, price_usd,
       supply_amount, supply_usd, supply_apy, liquidation_threshold, borrow_amount, borrow_usd, borrow_apy, utilization
from ranked where rn = 1
