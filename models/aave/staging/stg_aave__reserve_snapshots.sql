-- Rates and ratios arrive as fractions from both Aave APIs; the house unit is percent.
select snapshot_id, run_id, source_id, fetched_at, (fetched_at at time zone 'UTC')::date as day, version, chain_id, chain_name, market_name,
       coalesce(market_address, 'v4-hub') as market_key, market_address, underlying_address, symbol, decimals, price_usd,
       supply_amount, supply_usd, supply_apy * 100 as supply_apy, liquidation_threshold * 100 as liquidation_threshold,
       borrow_amount, borrow_usd, borrow_apy * 100 as borrow_apy, utilization * 100 as utilization
from {{ source('aave_raw', 'raw_reserve_snapshots') }}
where underlying_address is not null and underlying_address <> ''        -- the first v4 probe rows carried no asset address
  and not (version = 'v4' and market_address is null)                    -- and no spoke; v4 is keyed by spoke
