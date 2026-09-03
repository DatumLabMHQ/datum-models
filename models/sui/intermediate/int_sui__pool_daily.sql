-- Last snapshot of each pool per UTC day. "Last value of the day" is the house rule for daily grain.
with ranked as (
  select s.*,
         row_number() over (partition by protocol, symbol, day order by fetched_at desc) as rn
  from {{ ref('stg_sui__pool_snapshots') }} s
)
select * from ranked where rn = 1
