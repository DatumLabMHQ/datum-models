{{ config(materialized='incremental', unique_key=['day', 'chain_id', 'market_id', 'side', 'user_address'], incremental_strategy='delete+insert', on_schema_change='append_new_columns') }}
-- One row per sampled position per UTC day: the largest suppliers and borrowers of the largest listed
-- markets, from the last positions run of the day (scripts/ingest_morpho_positions.py, twice a day).
-- A sample, not the book: share_of_market_pct is against the market's own total at that snapshot, so
-- summing a market's rows on one side says how much of it the sample covers. The market total and its
-- positions come from two API reads seconds apart, so a share can land a little over 100; it is capped
-- there. Incremental like the market mart: raw rows older than 60 days tier to R2, so only the last 3
-- days are recomputed each run.
with snaps as (
  select * from {{ ref('stg_morpho__position_snapshots') }}
  {% if is_incremental() %} where day >= current_date - 3 {% endif %}
),
last_run as (
  select day, chain_id, market_id, max(fetched_at) as last_at from snaps group by 1, 2, 3
)
select s.day, s.fetched_at as as_of, s.chain_id, s.market_id, s.side, s.rank, s.user_address,
       s.supply_assets_usd, s.borrow_assets_usd, s.collateral_usd, s.health_factor, s.price_to_liquidation,
       least(100, case when s.side = 'supply' and s.market_supply_usd > 0 then s.supply_assets_usd / s.market_supply_usd * 100
                       when s.side = 'borrow' and s.market_borrow_usd > 0 then s.borrow_assets_usd / s.market_borrow_usd * 100 end) as share_of_market_pct,
       s.market_supply_usd, s.market_borrow_usd
from snaps s join last_run l on l.day = s.day and l.chain_id = s.chain_id and l.market_id = s.market_id and l.last_at = s.fetched_at
