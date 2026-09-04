with ranked as (
  select slug, chain, day, tvl_usd, borrowed_usd, fetched_at, run_id, row_number() over (partition by slug, chain, day order by fetched_at desc) as rn
  from {{ source('ref', 'raw_defillama_tvl') }}
  where slug in ('morpho-blue', 'aave-v3', 'aave-v2', 'sparklend', 'compound-v3', 'compound-v2', 'fluid-lending', 'euler-v2', 'moonwell-lending', 'sky-lending', 'liquity-v1', 'liquity-v2')
)
select slug, chain, day, tvl_usd, borrowed_usd, fetched_at, run_id from ranked where rn = 1
