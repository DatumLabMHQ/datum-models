select snapshot_id, run_id, fetched_at, (fetched_at at time zone 'UTC')::date as day, chain_id, market_id, lower(user_address) as user_address, side, rank,
       supply_assets_usd, borrow_assets_usd, collateral_usd, supply_shares, borrow_shares, health_factor, price_to_liquidation,
       market_supply_usd, market_borrow_usd, state_timestamp
from {{ source('morpho_raw', 'raw_position_snapshots') }}
