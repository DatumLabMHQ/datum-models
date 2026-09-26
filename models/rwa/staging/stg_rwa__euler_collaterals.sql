-- Accepted collateral per Euler borrowable vault, one row per pair per read. LTVs are fractions.
select fetched_at, (fetched_at at time zone 'UTC')::date as day, run_id, chain_id, lower(borrow_vault) as borrow_vault, lower(collateral_vault) as collateral_vault,
       borrow_ltv, liquidation_ltv, debt_backed_usd
from {{ source('rwa_raw', 'raw_euler_collaterals') }}
