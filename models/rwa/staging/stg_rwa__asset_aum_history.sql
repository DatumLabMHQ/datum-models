select h.asset_id, h.ts, (h.ts at time zone 'UTC')::date as day, h.aum::double precision as aum_usd, h.source, a.display_ticker as ticker, a.name as asset_name, i.name as issuer
from {{ source('rwa_raw', 'raw_asset_aum_history') }} h
left join {{ source('rwa_raw', 'raw_asset') }} a on a.asset_id = h.asset_id
left join {{ source('rwa_raw', 'raw_issuer') }} i on i.issuer_id = a.issuer_id
