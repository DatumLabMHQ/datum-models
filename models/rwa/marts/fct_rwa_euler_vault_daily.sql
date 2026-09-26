{{ config(materialized='incremental', unique_key=['day'], incremental_strategy='delete+insert', on_schema_change='append_new_columns') }}
-- Each run replaces whole days (unique_key day), so a vault or pair that leaves the cluster leaves the day too.
-- One row per Euler vault in an RWA cluster per UTC day, from the day's last read. role: collateral (the vault's asset is a
-- tokenized RWA in seeds/rwa_euler_assets.csv), borrowable (a vault in the same cluster, usually a stablecoin), or candidate
-- (an RWA-looking asset outside the seed, with money in it: review and add to the seed; never counted). Incremental: last 3 days.
with last_run as (
  select day, max(fetched_at) as fetched_at from {{ ref('stg_rwa__euler_vaults') }}
  {% if is_incremental() %} where day >= current_date - 3 {% endif %} group by day
)
select v.day, v.fetched_at as as_of, v.chain_id, v.vault, v.asset_address, v.asset_symbol, s.asset_class, s.issuer, v.cluster, v.curator, v.role,
       v.supply_usd, v.borrow_usd, v.utilization, v.supply_apy, v.borrow_apy
from {{ ref('stg_rwa__euler_vaults') }} v
join last_run r on r.fetched_at = v.fetched_at
left join {{ ref('rwa_euler_assets') }} s on s.chain_id = v.chain_id and s.asset_address = v.asset_address
