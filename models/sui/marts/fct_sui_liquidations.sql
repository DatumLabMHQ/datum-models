{{ config(materialized='incremental', unique_key=['event_id'], incremental_strategy='delete+insert', on_schema_change='append_new_columns') }}
-- Incremental: raw rows older than 60 days move to R2 (tiering), so this mart keeps its own history and
-- only recomputes the last 3 days each run. A full refresh rebuilds only what is still hot in Neon.
-- One row per liquidation event across the five protocols.
select
  event_id, protocol, tx_digest, ts, day,
  liquidator, borrower,
  collateral_asset, collateral_amount, collateral_price, collateral_usd,
  debt_asset, debt_amount, debt_price, debt_usd,
  treasury_amount, gas_used_mist, gas_usd,
  -- gross liquidator margin: what was seized minus what was repaid, before gas
  collateral_usd - debt_usd as gross_margin_usd,
  fetched_at as as_of
from {{ ref('stg_sui__liquidations') }}
  {% if is_incremental() %} where ts >= current_date - 3 {% endif %}
