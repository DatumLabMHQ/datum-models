{{ config(materialized='table') }}
-- Our own token-level TVL per day beside DefiLlama's chain sum, with the divergence recorded.
with own as (
  select day, sum(tvl_usd) as own_tvl_usd, max(as_of) as as_of from {{ ref('fct_centrifuge_token_daily') }} group by 1
),
remote as (
  select day, sum(tvl_usd) as defillama_tvl_usd from {{ ref('stg_centrifuge__defillama_tvl') }} group by 1
)
select coalesce(o.day, r.day) as day, o.own_tvl_usd, r.defillama_tvl_usd,
       case when o.own_tvl_usd is not null and r.defillama_tvl_usd > 0 then (o.own_tvl_usd - r.defillama_tvl_usd) / r.defillama_tvl_usd end as divergence_vs_defillama, o.as_of
from own o full outer join remote r using (day)
