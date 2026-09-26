{{ config(materialized='incremental', unique_key=['day'], incremental_strategy='delete+insert', on_schema_change='append_new_columns',
          post_hook=["update {{ this }} t set rwa_aum_usd = a.rwa_aum_usd, rwa_assets = a.assets, rwa_issuers = a.issuers
                      from (select f.day, sum(f.aum_usd) as rwa_aum_usd, count(*) as assets, count(distinct m.issuer) as issuers
                            from {{ ref('fct_rwa_asset_aum_daily') }} f join {{ ref('rwa_asset_meta') }} m on m.ticker = f.ticker
                            where not m.is_stablecoin group by f.day) a
                      where a.day = t.day",
                     "update {{ this }} set holders = null where holders = 0"]) }}
-- depends_on: {{ ref('fct_rwa_asset_aum_daily') }}
-- rwa_aum_usd is restated after each build as the sum of the tokenized (non-stablecoin) assets in fct_rwa_asset_aum_daily,
-- so the headline and the asset list are one number from one place (all chains; see that model for scope per asset).
-- holders is null on days without a holder snapshot (the worker wrote 0, which read as nobody).
-- Incremental: raw rows older than 60 days move to R2 (tiering), so this mart keeps its own history and
-- only recomputes the last 3 days each run. A full refresh rebuilds only what is still hot in Neon.
-- One row per UTC day: last observed totals of the day (the worker snapshots every 5 minutes).
with ranked as (
  select *, row_number() over (partition by day order by ts desc) as rn from {{ ref('stg_rwa__market_history') }}
  {% if is_incremental() %} where day >= current_date - 3 {% endif %}
)
select day, ts as as_of, rwa_aum as rwa_aum_usd, stablecoin_aum as stablecoin_aum_usd, total_aum as total_aum_usd,
       horizon_supplied_usd, nullif(holders, 0) as holders, active_addresses, actions, issuers,
       null::bigint as rwa_assets, null::bigint as rwa_issuers
from ranked where rn = 1
