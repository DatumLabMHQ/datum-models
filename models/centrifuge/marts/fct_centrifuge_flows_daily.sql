{{ config(materialized='table') }}
-- Deposits, redemptions and net flow per token per UTC day from the executed/claimed investor transactions we hold.
-- Coverage starts at the oldest of the 1,000 most recent transactions at first run and grows forward from there.
select day, token_id, symbol, pool_id,
       sum(value_usd) filter (where direction = 'deposit') as deposits_usd,
       sum(value_usd) filter (where direction = 'redemption') as redemptions_usd,
       coalesce(sum(value_usd) filter (where direction = 'deposit'), 0) - coalesce(sum(value_usd) filter (where direction = 'redemption'), 0) as net_flow_usd,
       count(*) as transactions
from {{ ref('stg_centrifuge__investor_transactions') }}
group by 1, 2, 3, 4
