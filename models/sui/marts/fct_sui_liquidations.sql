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
