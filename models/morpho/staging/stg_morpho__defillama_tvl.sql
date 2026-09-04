with ranked as (
  select slug, chain, day, tvl_usd, borrowed_usd, fetched_at, run_id, row_number() over (partition by slug, chain, day order by fetched_at desc) as rn
  from {{ source('morpho_raw', 'raw_defillama_tvl') }}
)
select slug, chain, day, tvl_usd, borrowed_usd, fetched_at, run_id from ranked where rn = 1
