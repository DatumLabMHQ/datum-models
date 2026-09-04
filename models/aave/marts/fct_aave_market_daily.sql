{{ config(materialized='incremental', unique_key=['day', 'version', 'chain_id', 'market_key'], incremental_strategy='delete+insert', on_schema_change='append_new_columns') }}
-- One row per market per UTC day: reserve sums plus the API's own totalMarketSize (v3 only) for reconciliation.
with reserves as (
  select day, version, chain_id, chain_name, market_name, market_key, max(as_of) as as_of, count(*) as reserves,
         sum(supply_usd) as supply_usd, sum(borrow_usd) as borrow_usd,
         sum(supply_usd * supply_apy) / nullif(sum(supply_usd), 0) as supply_apy_weighted,
         sum(borrow_usd * borrow_apy) / nullif(sum(borrow_usd), 0) as borrow_apy_weighted
  from {{ ref('fct_aave_reserve_daily') }}
  {% if is_incremental() %} where day >= current_date - 3 {% endif %}
  group by 1, 2, 3, 4, 5, 6
),
api as (
  select day, version, chain_id, market_key, max(total_market_size_usd) as api_total_market_size_usd from {{ ref('stg_aave__market_snapshots') }}
  {% if is_incremental() %} where day >= current_date - 3 {% endif %}
  group by 1, 2, 3, 4
)
select r.day, r.as_of, r.version, r.chain_id, r.chain_name, r.market_name, r.market_key, r.reserves, r.supply_usd, r.borrow_usd, r.supply_usd - r.borrow_usd as net_usd,
       r.supply_apy_weighted, r.borrow_apy_weighted, a.api_total_market_size_usd
from reserves r left join api a using (day, version, chain_id, market_key)
