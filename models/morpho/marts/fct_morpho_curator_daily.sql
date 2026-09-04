{{ config(materialized='table') }}
-- Curator share of listed-vault TVL per day, and an estimated annual fee revenue = TVL x gross APY x performance fee.
select day, curator, count(*) as vaults, count(distinct chain_id) as chains, sum(total_assets_usd) as tvl_usd,
       sum(total_assets_usd) / nullif(sum(sum(total_assets_usd)) over (partition by day), 0) as share_of_vault_tvl,
       sum(total_assets_usd * (fee_pct / 100.0) * (coalesce(apy, 0) / 100.0)) as est_annual_fee_revenue_usd
from {{ ref('fct_morpho_vault_daily') }}
where listed
group by 1, 2
