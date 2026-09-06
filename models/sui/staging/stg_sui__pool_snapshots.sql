-- Same grain as raw (one row per pool per ingest run). Units fixed here and nowhere else:
-- percent as numbers (0..100), USD as doubles, UTC timestamps. See datum-context/house/units.md.
select
  id                                          as snapshot_id,
  run_id,
  source_id,
  fetched_at,
  (fetched_at at time zone 'UTC')::date       as day,
  lower(protocol)                             as protocol,
  symbol,
  coin_type,
  decimals,
  total_supply::double precision              as total_supply,
  total_supply_usd,
  total_borrows::double precision             as total_borrows,
  total_borrows_usd,
  available_liquidity::double precision       as available_liquidity,
  available_liquidity_usd,
  supply_apy,
  borrow_apy,
  incentive_supply_apy,
  incentive_borrow_apy,
  -- utilization is stored 0..100; derive it when the adapter left it null
  coalesce(utilization,
           case when total_supply_usd > 0 then 100.0 * total_borrows_usd / total_supply_usd end) as utilization,
  -- House unit is percent. Adapters disagree: NAVI reports basis points (8000), the legacy import
  -- stored percent (80), some SDKs return a fraction (0.8). Normalise every row.
  case when ltv > 100 then ltv / 100.0 when ltv <= 1 then ltv * 100.0 else ltv end as ltv,
  case when liquidation_threshold > 100 then liquidation_threshold / 100.0
       when liquidation_threshold <= 1 then liquidation_threshold * 100.0 else liquidation_threshold end as liquidation_threshold,
  price_usd,
  irm,
  -- Non-lending rows (Bucket PSM, V1 wrappers, saving pools) are kept but flagged so marts can exclude them.
  case when symbol like 'PSM-%' or symbol like 'V1-%' or symbol like 'V1PSM-%' or symbol like 'BKT-PSM-%'
         or symbol like 'BKT-SAVE-%' or symbol like 'BKT-SCOIN-%' or symbol like 'BKT-AF-%'
         or symbol like 'BKT-KRIYA-%' or symbol like 'SAVING-%'
       then false else true end               as is_lending_pool
from {{ source('sui_raw', 'raw_pool_snapshots') }}
