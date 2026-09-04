select snapshot_id, run_id, fetched_at, (fetched_at at time zone 'UTC')::date as day, chain_id, lower(vault_address) as vault_address, name, symbol, listed,
       asset_symbol, lower(asset_address) as asset_address, total_assets_usd, apy * 100 as apy, net_apy * 100 as net_apy, net_apy_excl_rewards * 100 as net_apy_excl_rewards,
       fee * 100 as fee_pct, share_price_usd, lower(curator_address) as curator_address, curator_ids, curator_names,
       coalesce(curator_names[1], 'UNATTRIBUTED') as curator, state_timestamp
from {{ source('morpho_raw', 'raw_vault_snapshots') }}
