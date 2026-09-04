{{ config(materialized='incremental', unique_key=['day', 'pool_id'], incremental_strategy='delete+insert', on_schema_change='append_new_columns') }}
with names as (
  select distinct on (pool_id) pool_id, name from {{ source('centrifuge_raw', 'raw_pool_snapshots') }} order by pool_id, fetched_at desc
)
select t.day, t.pool_id, n.name as pool_name, count(*) as tokens, sum(t.tvl_usd) as tvl_usd, max(t.as_of) as as_of
from {{ ref('fct_centrifuge_token_daily') }} t left join names n using (pool_id)
{% if is_incremental() %} where t.day >= current_date - 3 {% endif %}
group by 1, 2, 3
