{{ config(materialized='incremental', unique_key=['day', 'token_id'], incremental_strategy='delete+insert', on_schema_change='append_new_columns') }}
-- One row per token per UTC day. Our hourly snapshot wins when we have one; the API's own history fills the days before our first run.
with snap as (
  select day, token_id, pool_id, symbol, fetched_at as as_of, supply, price_usd, tvl_usd, 'snapshot' as basis,
         row_number() over (partition by token_id, day order by fetched_at desc) as rn
  from {{ ref('stg_centrifuge__token_snapshots') }}
  {% if is_incremental() %} where day >= current_date - 3 {% endif %}
),
hist as (
  select day, token_id, pool_id, symbol, ts as as_of, supply, price_usd, tvl_usd, 'api_history' as basis,
         row_number() over (partition by token_id, day order by ts desc) as rn
  from {{ ref('stg_centrifuge__token_history') }}
  {% if is_incremental() %} where day >= current_date - 3 {% endif %}
),
unioned as (
  select * from snap where rn = 1
  union all
  select * from hist h where rn = 1 and not exists (select 1 from snap s where s.rn = 1 and s.token_id = h.token_id and s.day = h.day)
)
select day, token_id, pool_id, symbol, as_of, supply, price_usd, tvl_usd, basis from unioned
