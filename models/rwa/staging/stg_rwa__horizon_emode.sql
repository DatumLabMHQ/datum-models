-- Horizon e-mode categories per reserve and role, one row per read, with day derived in UTC. Percent-as-number.
select fetched_at, (fetched_at at time zone 'UTC')::date as day, run_id, chain_id, pool, category_id, label,
       ltv_pct, liquidation_threshold_pct, liquidation_bonus_pct, lower(reserve) as reserve, symbol, role
from {{ source('rwa_raw', 'raw_horizon_emode') }}
