select ts, (ts at time zone 'UTC')::date as day, rwa_aum, stablecoin_aum, total_aum, horizon_supplied_usd, holders, active_addresses, actions, issuers, source_id, run_id
from {{ source('rwa_raw', 'raw_market_history') }}
