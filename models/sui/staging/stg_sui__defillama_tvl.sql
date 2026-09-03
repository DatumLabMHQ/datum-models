select
  lower(protocol) as protocol,
  slug,
  day,
  tvl_usd,
  fetched_at
from {{ source('sui_raw', 'raw_defillama_tvl') }}
