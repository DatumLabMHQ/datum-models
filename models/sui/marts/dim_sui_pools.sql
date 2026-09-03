-- One row per pool we have ever seen, with its latest descriptors.
with latest as (
  select *, row_number() over (partition by protocol, symbol order by fetched_at desc) as rn
  from {{ ref('stg_sui__pool_snapshots') }}
)
select
  protocol,
  symbol,
  coin_type,
  decimals,
  is_lending_pool,
  fetched_at as last_seen_at
from latest where rn = 1
