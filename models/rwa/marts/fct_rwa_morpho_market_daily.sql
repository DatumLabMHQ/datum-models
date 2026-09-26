{{ config(materialized='incremental', unique_key=['day'], incremental_strategy='delete+insert', on_schema_change='append_new_columns',
          post_hook=["update {{ this }} set chain_id = 1 where chain_id is null",
                     "delete from {{ this }} where coalesce(source, 'legacy_worker') = 'legacy_worker' and day >= '" ~ var('rwa_morpho_native_from', '2026-09-05') ~ "'"]) }}
-- One row per Morpho market with RWA collateral per UTC day, last value of the day. Fractions: lltv, utilization, borrow_apy.
-- Two sources, one row shape:
--   * up to the day before rwa_morpho_native_from: the legacy worker's curated list (9 Ethereum markets), imported from its database;
--   * from rwa_morpho_native_from on: every market on every chain whose collateral address is in seeds/rwa_morpho_collateral.csv,
--     read from the platform's own Morpho mart. The seed is the RWA definition; add an address there to add a token.
-- Incremental: recomputes the last 3 days, or everything since var rwa_backfill_from when it is set (one-off backfills).
{% set native_from = var('rwa_morpho_native_from', '2026-09-05') %}
{% set since = "date '" ~ var('rwa_backfill_from') ~ "'" if var('rwa_backfill_from', none) else 'current_date - 3' %}
with legacy as (
  select *, row_number() over (partition by market_id, day order by ts desc) as rn from {{ ref('stg_rwa__morpho_market_history') }}
  where day < date '{{ native_from }}' {% if is_incremental() %} and day >= {{ since }} {% endif %}
), native as (
  select m.day, m.as_of, m.chain_id, m.market_id, m.listed, m.collateral_symbol, lower(m.collateral_address) as collateral_address, m.loan_symbol,
         s.asset_class, s.issuer, m.lltv, m.collateral_assets_usd, m.borrow_assets_usd, m.supply_assets_usd, m.utilization / 100.0 as utilization, m.borrow_apy / 100.0 as borrow_apy
  from {{ ref('fct_morpho_market_daily') }} m
  join {{ ref('rwa_morpho_collateral') }} s on s.chain_id = m.chain_id and s.collateral_address = lower(m.collateral_address)
  where m.day >= date '{{ native_from }}' {% if is_incremental() %} and m.day >= {{ since }} {% endif %}
    -- Unlisted markets are kept only when they look like real markets: debt no larger than the collateral behind it.
    -- An unlisted PAXG/USDC market reported $10.6B of debt against no collateral on 2026-09-26 (broken oracle or test market).
    and (m.listed or coalesce(m.borrow_assets_usd, 0) <= coalesce(m.collateral_assets_usd, 0))
)
select day, ts as as_of, 1 as chain_id, market_id, true as listed, collateral_symbol, null::text as collateral_address, loan_symbol, asset_class, null::text as issuer,
       lltv, collateral_usd, borrow_usd, null::double precision as supply_usd, utilization, borrow_apy, 'legacy_worker' as source
from legacy where rn = 1
union all
select day, as_of, chain_id, market_id, listed, collateral_symbol, collateral_address, loan_symbol, asset_class, issuer,
       lltv, collateral_assets_usd, borrow_assets_usd, supply_assets_usd, utilization, borrow_apy, 'platform_morpho' as source
from native
