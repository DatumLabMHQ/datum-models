select snapshot_id, run_id, fetched_at, (fetched_at at time zone 'UTC')::date as day, chain_id, market_id, listed,
       collateral_symbol, lower(collateral_address) as collateral_address, loan_symbol, lower(loan_address) as loan_address, loan_decimals,
       lltv::double precision / 1e18 as lltv, supply_assets_usd, borrow_assets_usd, collateral_assets_usd, liquidity_assets_usd,
       utilization * 100 as utilization, supply_apy * 100 as supply_apy, borrow_apy * 100 as borrow_apy, net_supply_apy * 100 as net_supply_apy,
       fee * 100 as fee_pct, bad_debt_usd, state_timestamp
from {{ source('morpho_raw', 'raw_market_snapshots') }}
