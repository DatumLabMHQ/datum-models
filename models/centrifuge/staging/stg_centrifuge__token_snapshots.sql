-- totalIssuance is in token decimals; tokenPrice is always 18 decimals; pools are USD-denominated (currency 840).
select snapshot_id, run_id, fetched_at, (fetched_at at time zone 'UTC')::date as day, pool_id, token_id, symbol, name, decimals, is_active,
       total_issuance / power(10, decimals) as supply, token_price / 1e18 as price_usd,
       (total_issuance / power(10, decimals)) * (token_price / 1e18) as tvl_usd, instances
from {{ source('centrifuge_raw', 'raw_token_snapshots') }}
