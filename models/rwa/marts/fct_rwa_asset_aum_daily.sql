{{ config(materialized='table') }}
-- One row per tokenized asset per UTC day: last AUM observation of the day. History back to 2024 for the seeded assets.
with ranked as (
  select *, row_number() over (partition by asset_id, day order by ts desc) as rn from {{ ref('stg_rwa__asset_aum_history') }}
)
select day, ts as as_of, asset_id, ticker, asset_name, issuer, aum_usd, source from ranked where rn = 1
