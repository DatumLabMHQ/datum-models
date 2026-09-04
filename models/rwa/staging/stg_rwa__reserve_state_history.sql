select ts, (ts at time zone 'UTC')::date as day, lower(reserve) as reserve, symbol, supplied_usd, borrowed_usd, supply_apy_pct as supply_apy, borrow_apy_pct as borrow_apy,
       utilization_pct as utilization, ltv_pct as ltv, liq_threshold_pct as liquidation_threshold, oracle_price, nav, source_id, run_id
from {{ source('rwa_raw', 'raw_reserve_state_history') }}
