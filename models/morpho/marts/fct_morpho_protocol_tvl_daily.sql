{{ config(materialized='table') }}
-- DefiLlama per-chain TVL for Morpho and comparators, with each protocol's share of the tracked lending set on that chain.
with base as (
  select slug, chain, day, tvl_usd, borrowed_usd from {{ ref('stg_morpho__defillama_tvl') }}
),
totals as (
  select chain, day, sum(tvl_usd) as tracked_lending_tvl_usd from base group by 1, 2
)
select b.slug, b.chain, b.day, b.tvl_usd as tvl_net_usd, b.tvl_usd + coalesce(b.borrowed_usd, 0) as tvl_gross_usd, b.borrowed_usd,
       b.tvl_usd / nullif(t.tracked_lending_tvl_usd, 0) as share_of_tracked_lending
from base b join totals t using (chain, day)
