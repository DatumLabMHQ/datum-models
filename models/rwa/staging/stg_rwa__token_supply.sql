-- totalSupply per tokenized asset and chain, one row per read (a run writes every chain or nothing).
select fetched_at, (fetched_at at time zone 'UTC')::date as day, run_id, ticker, chain, address, total_supply::double precision as total_supply
from {{ source('rwa_raw', 'raw_token_supply') }}
