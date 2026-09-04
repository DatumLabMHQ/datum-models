{{ config(materialized='table') }}
with ranked as (
  select *, row_number() over (partition by chain_id, vault_address, day order by fetched_at desc) as rn from {{ ref('stg_morpho__vault_snapshots') }}
)
select day, fetched_at as as_of, chain_id, vault_address, name, symbol, listed, asset_symbol, total_assets_usd, apy, net_apy, net_apy_excl_rewards, fee_pct,
       share_price_usd, curator, curator_ids, curator_names
from ranked where rn = 1
