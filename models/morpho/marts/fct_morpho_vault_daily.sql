{{ config(materialized='incremental', unique_key=['day', 'chain_id', 'vault_address'], incremental_strategy='delete+insert', on_schema_change='append_new_columns') }}
-- Incremental: raw rows older than 60 days move to R2 (tiering), so this mart keeps its own history and
-- only recomputes the last 3 days each run. A full refresh rebuilds only what is still hot in Neon.
with ranked as (
  select *, row_number() over (partition by chain_id, vault_address, day order by fetched_at desc) as rn from {{ ref('stg_morpho__vault_snapshots') }}
  {% if is_incremental() %} where day >= current_date - 3 {% endif %}
)
select day, fetched_at as as_of, chain_id, vault_address, vault_version, name, symbol, listed, asset_symbol, total_assets_usd, apy, net_apy, net_apy_excl_rewards, fee_pct, management_fee_pct, idle_assets_usd,
       share_price_usd, curator, curator_ids, curator_names
from ranked where rn = 1
