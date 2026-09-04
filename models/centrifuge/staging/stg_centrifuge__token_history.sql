-- The API's own history per token, scaled with the token's decimals from its latest snapshot.
with dec as (
  select distinct on (token_id) token_id, pool_id, symbol, decimals from {{ source('centrifuge_raw', 'raw_token_snapshots') }} order by token_id, fetched_at desc
)
select h.token_id, d.pool_id, d.symbol, h.ts, (h.ts at time zone 'UTC')::date as day,
       h.total_issuance / power(10, d.decimals) as supply, h.token_price / 1e18 as price_usd,
       (h.total_issuance / power(10, d.decimals)) * (h.token_price / 1e18) as tvl_usd
from {{ source('centrifuge_raw', 'raw_token_history') }} h join dec d using (token_id)
