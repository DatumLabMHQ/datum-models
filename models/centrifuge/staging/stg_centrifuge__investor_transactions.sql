with tok as (
  select distinct on (token_id) token_id, symbol, decimals from {{ source('centrifuge_raw', 'raw_token_snapshots') }} order by token_id, fetched_at desc
),
pool as (
  select distinct on (pool_id) pool_id, decimals as pool_decimals from {{ source('centrifuge_raw', 'raw_pool_snapshots') }} order by pool_id, fetched_at desc
)
select t.event_key, t.tx_hash, t.type, t.account, t.pool_id, t.token_id, k.symbol, t.chain_id, t.chain_name, t.created_at, (t.created_at at time zone 'UTC')::date as day,
       case when t.type in ('SYNC_DEPOSIT', 'DEPOSIT_CLAIMED', 'DEPOSIT_REQUEST_EXECUTED') then 'deposit' else 'redemption' end as direction,
       t.token_amount / power(10, coalesce(k.decimals, 18)) as token_amount, t.currency_amount / power(10, coalesce(p.pool_decimals, 6)) as currency_amount, t.token_price / 1e18 as price_usd,
       (t.token_amount / power(10, coalesce(k.decimals, 18))) * (t.token_price / 1e18) as value_usd
from {{ source('centrifuge_raw', 'raw_investor_transactions') }} t left join tok k using (token_id) left join pool p using (pool_id)
