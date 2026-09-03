-- Latest fetch per protocol per day. Raw keeps every fetch (append-only); this view collapses them.
with ranked as (
  select lower(protocol) as protocol, slug, day, tvl_usd, fetched_at, run_id,
         row_number() over (partition by protocol, day order by fetched_at desc) as rn
  from {{ source('sui_raw', 'raw_defillama_tvl') }}
)
select protocol, slug, day, tvl_usd, fetched_at, run_id from ranked where rn = 1
