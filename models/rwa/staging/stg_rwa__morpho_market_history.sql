select ts, (ts at time zone 'UTC')::date as day, market_id, collateral_symbol, loan_symbol, asset_class, lltv, collateral_usd, borrow_usd, utilization, borrow_apy, source_id, run_id
from {{ source('rwa_raw', 'raw_morpho_market_history') }}
