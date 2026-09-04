select snapshot_id, run_id, fetched_at, (fetched_at at time zone 'UTC')::date as day, version, chain_id, chain_name, market_name,
       coalesce(market_address, 'v4-hub') as market_key, total_market_size_usd, reserve_count
from {{ source('aave_raw', 'raw_market_snapshots') }}
