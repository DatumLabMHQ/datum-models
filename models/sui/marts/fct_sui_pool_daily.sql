-- One row per protocol, pool, UTC day. The only pool-level table products read.
select
  protocol,
  symbol,
  day,
  is_lending_pool,
  total_supply,
  total_supply_usd,
  total_borrows,
  total_borrows_usd,
  available_liquidity_usd,
  supply_apy,
  borrow_apy,
  incentive_supply_apy,
  incentive_borrow_apy,
  utilization,
  ltv,
  liquidation_threshold,
  price_usd,
  fetched_at as as_of,
  run_id
from {{ ref('int_sui__pool_daily') }}
