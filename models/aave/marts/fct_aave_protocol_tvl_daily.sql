{{ config(materialized='table') }}
-- DefiLlama per-chain TVL for Aave v2, v3 and v4 (net = DefiLlama tvl, gross = tvl + borrowed). Reconciliation surface, not our own count.
select slug, chain, day, tvl_usd as tvl_net_usd, tvl_usd + coalesce(borrowed_usd, 0) as tvl_gross_usd, borrowed_usd, fetched_at as as_of
from {{ ref('stg_aave__defillama_tvl') }}
