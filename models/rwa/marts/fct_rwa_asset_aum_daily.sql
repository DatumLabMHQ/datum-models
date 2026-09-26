{{ config(materialized='incremental', unique_key=['asset_id', 'day'], incremental_strategy='delete+insert', on_schema_change='append_new_columns') }}
-- Incremental: raw rows older than 60 days move to R2 (tiering), so this mart keeps its own history and only recomputes
-- the last 3 days each run, or everything since var rwa_backfill_from when it is set (one-off backfills).
-- One row per tokenized asset per UTC day. AUM counts every chain the asset is issued on (aum_scope):
--   issuer_api  USYC, USTB, USCC: the issuer's own AUM figure, all chains (legacy worker import);
--   all_chains  JTRSY, JAAA: the Centrifuge product's token value (supply on every chain times token price);
--               ACRED, VBILL, mGLOBAL: totalSupply on every chain in seeds/rwa_token_registry.csv times Horizon's NAV,
--               from 2026-09-26 (earlier days keep the Ethereum-only figure and say so);
--   ethereum    stablecoins, which are context here rather than the asset universe.
{% set since = "date '" ~ var('rwa_backfill_from') ~ "'" if var('rwa_backfill_from', none) else 'current_date - 3' %}
with legacy as (
  select *, row_number() over (partition by asset_id, day order by ts desc) as rn from {{ ref('stg_rwa__asset_aum_history') }}
  {% if is_incremental() %} where day >= {{ since }} {% endif %}
), legacy_last as (
  select day, ts as as_of, asset_id, ticker, asset_name, issuer, aum_usd, source from legacy where rn = 1
), assets as (
  select asset_id, display_ticker as ticker, name as asset_name from {{ source('rwa_raw', 'raw_asset') }}
  union all select 1011, 'mGLOBAL', 'Midas Fasanara Global'   -- listed on Horizon in September 2026; not in the legacy registry
), days as (
  select distinct day from legacy_last
), nav as (
  select distinct on (day, symbol) day, symbol, nav from {{ ref('stg_rwa__reserve_state_history') }}
  where nav > 0 {% if is_incremental() %} and day >= {{ since }} {% endif %}
  order by day, symbol, ts desc
), supply_runs as (
  select day, max(fetched_at) as fetched_at from {{ ref('stg_rwa__token_supply') }} group by day
), supply as (   -- the last complete run on or before each day, carried forward with its own timestamp
  select d.day, s.ticker, sum(s.total_supply) as supply, count(*) as chains, max(s.fetched_at) as as_of
  from days d
  join lateral (select r.fetched_at from supply_runs r where r.day <= d.day order by r.day desc limit 1) lr on true
  join {{ ref('stg_rwa__token_supply') }} s on s.fetched_at = lr.fetched_at
  group by 1, 2
), overrides as (
  select s.day, s.as_of, s.ticker, s.supply * n.nav as aum_usd, 'chain_rpc_x_horizon_nav' as source, s.chains
  from supply s join nav n on n.day = s.day and n.symbol = s.ticker
  union all
  select c.day, c.as_of, c.symbol, c.tvl_usd, 'centrifuge_api', null::bigint
  from {{ ref('fct_centrifuge_token_daily') }} c
  where c.symbol in ('JTRSY', 'JAAA') {% if is_incremental() %} and c.day >= {{ since }} {% endif %}
)
select l.day, l.as_of, l.asset_id, l.ticker, l.asset_name, coalesce(m.issuer, l.issuer) as issuer, l.aum_usd, l.source,
       m.asset_class, m.is_stablecoin, case when m.aum_scope = 'issuer_api' then 'issuer_api' else 'ethereum' end as aum_scope, null::bigint as chains
from legacy_last l
left join {{ ref('rwa_asset_meta') }} m on m.ticker = l.ticker
where not exists (select 1 from overrides o where o.day = l.day and o.ticker = l.ticker)
union all
select o.day, o.as_of, a.asset_id, o.ticker, a.asset_name, m.issuer, o.aum_usd, o.source, m.asset_class, m.is_stablecoin, 'all_chains', o.chains
from overrides o
join assets a on a.ticker = o.ticker
left join {{ ref('rwa_asset_meta') }} m on m.ticker = o.ticker
