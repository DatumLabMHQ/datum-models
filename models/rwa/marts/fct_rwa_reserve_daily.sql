{{ config(materialized='incremental', unique_key=['reserve', 'day'], incremental_strategy='delete+insert', on_schema_change='append_new_columns') }}
-- Incremental: raw rows older than 60 days move to R2 (tiering), so this mart keeps its own history and
-- only recomputes the last 3 days each run. A full refresh rebuilds only what is still hot in Neon.
-- One row per Horizon reserve per UTC day, last value of the day. ltv and liquidation_threshold are the reserve's
-- base configuration; emode_* are the parameters of the e-mode category the reserve is collateral in (the one with
-- the highest LTV if several), which is how Horizon is borrowed against. E-mode columns start on 2026-09-26.
with ranked as (
  select *, row_number() over (partition by reserve, day order by ts desc) as rn from {{ ref('stg_rwa__reserve_state_history') }}
  {% if is_incremental() %} where day >= current_date - 3 {% endif %}
), last_emode as (
  select * from {{ ref('stg_rwa__horizon_emode') }} e
  where fetched_at = (select max(fetched_at) from {{ ref('stg_rwa__horizon_emode') }} x where x.day = e.day)
), emode as (
  select distinct on (c.day, c.reserve) c.day, c.reserve, c.category_id, c.label, c.ltv_pct, c.liquidation_threshold_pct, c.liquidation_bonus_pct,
         (select string_agg(b.symbol, ', ' order by b.symbol) from last_emode b where b.day = c.day and b.category_id = c.category_id and b.role = 'borrowable') as borrowable
  from last_emode c where c.role = 'collateral'
  order by c.day, c.reserve, c.ltv_pct desc
)
select r.day, r.ts as as_of, r.reserve, r.symbol, r.supplied_usd, r.borrowed_usd, r.supply_apy, r.borrow_apy, r.utilization, r.ltv, r.liquidation_threshold,
       r.oracle_price, r.nav, e.category_id as emode_category, e.label as emode_label, e.ltv_pct as emode_ltv, e.liquidation_threshold_pct as emode_liquidation_threshold,
       e.liquidation_bonus_pct as emode_liquidation_bonus, e.borrowable as emode_borrowable
from ranked r left join emode e on e.day = r.day and e.reserve = r.reserve
where r.rn = 1
