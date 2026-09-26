-- Euler v2 vaults in RWA clusters (and RWA-looking candidates), one row per vault per read. APYs and utilization as the API sends them.
select fetched_at, (fetched_at at time zone 'UTC')::date as day, run_id, chain_id, lower(vault) as vault, lower(asset_address) as asset_address, asset_symbol,
       supply_usd, borrow_usd, utilization, supply_apy, borrow_apy, cluster, curator, role, discovered
from {{ source('rwa_raw', 'raw_euler_vaults') }}
