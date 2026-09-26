{{ config(materialized='incremental', unique_key=['day'], incremental_strategy='delete+insert', on_schema_change='append_new_columns') }}
-- Each run replaces whole days (unique_key day), so a vault or pair that leaves the cluster leaves the day too.
-- One row per Euler lending pair in an RWA cluster per UTC day: a borrowable vault, a collateral it accepts, the borrow and
-- liquidation LTV (fractions) and the debt that collateral backs (USD). A pair is RWA when either side's asset is a tokenized RWA.
-- This is the composability map for Euler: which RWA can be borrowed against, for what, how far, and how much is.
with last_run as (
  select day, max(fetched_at) as fetched_at from {{ ref('stg_rwa__euler_collaterals') }}
  {% if is_incremental() %} where day >= current_date - 3 {% endif %} group by day
), vaults as (
  select * from {{ ref('fct_rwa_euler_vault_daily') }} {% if is_incremental() %} where day >= current_date - 3 {% endif %}
)
select c.day, c.fetched_at as as_of, c.chain_id, b.cluster, b.curator, c.borrow_vault, b.asset_symbol as borrow_symbol, c.collateral_vault,
       v.asset_symbol as collateral_symbol, v.asset_class as collateral_class, v.issuer as collateral_issuer, b.asset_class as borrow_class,
       c.borrow_ltv, c.liquidation_ltv, coalesce(c.debt_backed_usd, 0) as debt_backed_usd, v.supply_usd as collateral_vault_supply_usd
from {{ ref('stg_rwa__euler_collaterals') }} c
join last_run r on r.fetched_at = c.fetched_at
join vaults b on b.day = c.day and b.chain_id = c.chain_id and b.vault = c.borrow_vault
left join vaults v on v.day = c.day and v.chain_id = c.chain_id and v.vault = c.collateral_vault
where coalesce(v.asset_class, b.asset_class) is not null
