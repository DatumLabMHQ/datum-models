{{ config(materialized='incremental', unique_key=['day', 'chain_id', 'market_id'], incremental_strategy='delete+insert', on_schema_change='append_new_columns') }}
-- One row per sampled market per UTC day: its largest borrowers' collateral by health-factor band, how
-- much of the market's borrow that sample covers, and how concentrated its supply is. The bands are the
-- ones the dashboards draw; below 1.05 is what a small price move would liquidate. Borrowers with no
-- reported health factor count in collateral_tracked_usd but in no band. Coverage and share ratios are
-- capped at 100: the market total and its positions are read seconds apart, so a full sample can add up
-- to a little more than the market it came from.
with p as (
  select * from {{ ref('fct_morpho_position_daily') }}
  {% if is_incremental() %} where day >= current_date - 3 {% endif %}
),
base as (
  select day, chain_id, market_id, max(as_of) as as_of, max(market_supply_usd) as market_supply_usd, max(market_borrow_usd) as market_borrow_usd
  from p group by 1, 2, 3
),
b as (
  select day, chain_id, market_id, count(*) as borrowers_tracked, sum(borrow_assets_usd) as borrow_tracked_usd, sum(collateral_usd) as collateral_tracked_usd,
         sum(case when health_factor < 1.05 then collateral_usd else 0 end) as hf_below_1_05_usd,
         sum(case when health_factor >= 1.05 and health_factor < 1.25 then collateral_usd else 0 end) as hf_1_05_to_1_25_usd,
         sum(case when health_factor >= 1.25 and health_factor < 1.5 then collateral_usd else 0 end) as hf_1_25_to_1_5_usd,
         sum(case when health_factor >= 1.5 and health_factor < 2 then collateral_usd else 0 end) as hf_1_5_to_2_usd,
         sum(case when health_factor >= 2 then collateral_usd else 0 end) as hf_above_2_usd,
         min(health_factor) as min_health_factor
  from p where side = 'borrow' and borrow_assets_usd > 0 group by 1, 2, 3
),
s as (
  select day, chain_id, market_id, count(*) as suppliers_tracked, sum(supply_assets_usd) as supply_tracked_usd,
         sum(case when rank <= 5 then supply_assets_usd else 0 end) as top5_supply_usd
  from p where side = 'supply' and supply_assets_usd > 0 group by 1, 2, 3
)
select base.day, base.as_of, base.chain_id, base.market_id, base.market_supply_usd, base.market_borrow_usd,
       coalesce(b.borrowers_tracked, 0) as borrowers_tracked, coalesce(b.borrow_tracked_usd, 0) as borrow_tracked_usd,
       least(100, case when base.market_borrow_usd > 0 then coalesce(b.borrow_tracked_usd, 0) / base.market_borrow_usd * 100 end) as borrow_coverage_pct,
       coalesce(b.collateral_tracked_usd, 0) as collateral_tracked_usd,
       coalesce(b.hf_below_1_05_usd, 0) as hf_below_1_05_usd, coalesce(b.hf_1_05_to_1_25_usd, 0) as hf_1_05_to_1_25_usd, coalesce(b.hf_1_25_to_1_5_usd, 0) as hf_1_25_to_1_5_usd,
       coalesce(b.hf_1_5_to_2_usd, 0) as hf_1_5_to_2_usd, coalesce(b.hf_above_2_usd, 0) as hf_above_2_usd, b.min_health_factor,
       coalesce(s.suppliers_tracked, 0) as suppliers_tracked, coalesce(s.supply_tracked_usd, 0) as supply_tracked_usd,
       least(100, case when base.market_supply_usd > 0 then coalesce(s.supply_tracked_usd, 0) / base.market_supply_usd * 100 end) as supply_coverage_pct,
       least(100, case when base.market_supply_usd > 0 then coalesce(s.top5_supply_usd, 0) / base.market_supply_usd * 100 end) as top5_supply_share_pct
from base
left join b on b.day = base.day and b.chain_id = base.chain_id and b.market_id = base.market_id
left join s on s.day = base.day and s.chain_id = base.chain_id and s.market_id = base.market_id
