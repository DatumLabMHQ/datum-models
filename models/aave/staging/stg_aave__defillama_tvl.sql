with ranked as (
  select slug, chain, day, tvl_usd, borrowed_usd, fetched_at, run_id, row_number() over (partition by slug, chain, day order by fetched_at desc) as rn
  from {{ source('ref', 'raw_defillama_tvl') }} where slug in ('aave-v3', 'aave-v2', 'aave-v4')
)
select slug, chain, day, tvl_usd, borrowed_usd, fetched_at, run_id from ranked where rn = 1
