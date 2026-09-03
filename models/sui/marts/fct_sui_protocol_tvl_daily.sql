-- Net and gross TVL per protocol per day, with the method recorded beside the value.
-- Method per protocol follows datum-context/metrics/tvl-net.md: NAVI, Suilend, AlphaLend are
-- computed from our own pool rows (net = supply - borrow); Scallop and Bucket are 'remote'
-- (DefiLlama) because their UIs count LP wrappers we do not unwrap yet.
with own as (
  select
    protocol, day,
    sum(total_supply_usd)                              as tvl_gross_usd,
    sum(total_supply_usd) - sum(total_borrows_usd)     as tvl_net_usd,
    sum(total_borrows_usd)                             as borrows_usd,
    max(as_of)                                         as as_of
  from {{ ref('fct_sui_pool_daily') }}
  where is_lending_pool
  group by 1, 2
),
remote as (
  select protocol, day, tvl_usd, fetched_at as as_of
  from {{ ref('stg_sui__defillama_tvl') }}
),
method as (
  select * from (values ('navi','net'), ('suilend','net'), ('alphalend','net'), ('scallop','remote'), ('bucket','remote')) as m(protocol, method)
),
days as (
  select protocol, day from own
  union
  select protocol, day from remote
)
select
  d.protocol,
  d.day,
  m.method,
  case when m.method = 'remote' then r.tvl_usd else o.tvl_net_usd end        as tvl_net_usd,
  case when m.method = 'remote' then null     else o.tvl_gross_usd end      as tvl_gross_usd,
  case when m.method = 'remote' then null     else o.borrows_usd end        as borrows_usd,
  r.tvl_usd                                                                 as defillama_tvl_usd,
  case when o.tvl_net_usd is not null and r.tvl_usd > 0
       then (o.tvl_net_usd - r.tvl_usd) / r.tvl_usd end                     as divergence_vs_defillama,
  coalesce(o.as_of, r.as_of)                                                as as_of
from days d
join method m on m.protocol = d.protocol
left join own o on o.protocol = d.protocol and o.day = d.day
left join remote r on r.protocol = d.protocol and r.day = d.day
where (m.method = 'remote' and r.tvl_usd is not null) or (m.method = 'net' and o.tvl_net_usd is not null)
