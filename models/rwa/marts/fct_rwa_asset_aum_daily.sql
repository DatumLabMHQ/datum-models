{{ config(materialized='incremental', unique_key=['asset_id', 'day'], incremental_strategy='delete+insert', on_schema_change='append_new_columns') }}
-- Incremental: raw rows older than 60 days move to R2 (tiering), so this mart keeps its own history and
-- only recomputes the last 3 days each run. A full refresh rebuilds only what is still hot in Neon.
-- One row per tokenized asset per UTC day: last AUM observation of the day. History back to 2024 for the seeded assets.
with ranked as (
  select *, row_number() over (partition by asset_id, day order by ts desc) as rn from {{ ref('stg_rwa__asset_aum_history') }}
  {% if is_incremental() %} where day >= current_date - 3 {% endif %}
)
select day, ts as as_of, asset_id, ticker, asset_name, issuer, aum_usd, source from ranked where rn = 1
