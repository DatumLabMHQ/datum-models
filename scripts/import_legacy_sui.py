"""One-time import of the months of Sui history held in the existing SuiLending Neon database
(Prisma tables PoolSnapshot, LiquidationEvent, DefillamaTvl) into the platform's raw layer.

Read-only against the legacy database. Rows are appended with source_id = 'legacy_neon' and a
receipt in ops.legacy_imports. Idempotent: liquidation events dedupe on event_id; pool
snapshots and DefiLlama rows are skipped when a legacy row for the same key already exists.

  LEGACY_SUI_DATABASE_URL=postgresql://...   python scripts/import_legacy_sui.py [--since YYYY-MM-DD]
"""
import argparse, os, sys, json
import psycopg2, psycopg2.extras

p = argparse.ArgumentParser(); p.add_argument('--since', default='2026-01-01'); a = p.parse_args()
src_url = os.environ.get('LEGACY_SUI_DATABASE_URL'); dst_url = os.environ.get('DATABASE_URL_DIRECT') or os.environ.get('DATABASE_URL')
if not src_url or not dst_url: sys.exit('LEGACY_SUI_DATABASE_URL and DATABASE_URL must be set')
src = psycopg2.connect(src_url); dst = psycopg2.connect(dst_url)
s = src.cursor(cursor_factory=psycopg2.extras.RealDictCursor, name='legacy'); d = dst.cursor()

def receipt(source_table, target_table):
    d.execute("insert into ops.legacy_imports (product, source_db, source_table, target_table) values ('sui','suilending-neon',%s,%s) returning import_id", (source_table, target_table))
    dst.commit(); return d.fetchone()[0]
def close(iid, read, written, mn, mx, status='ok', error=None):
    d.execute("update ops.legacy_imports set finished_at=now(), rows_read=%s, rows_written=%s, min_ts=%s, max_ts=%s, status=%s, error=%s where import_id=%s", (read, written, mn, mx, status, error, iid)); dst.commit()

# ---- one run row so provenance is complete
d.execute("insert into ops.sync_runs (job, product, runner, notes) values ('sui.import_legacy','sui','local','{}') returning run_id"); run_id = d.fetchone()[0]; dst.commit()

# ---- PoolSnapshot -> raw_pool_snapshots
iid = receipt('PoolSnapshot', 'sui.raw_pool_snapshots'); read = written = 0; mn = mx = None
try:
    s.execute('select * from "PoolSnapshot" where "timestamp" >= %s order by "timestamp"', (a.since,))
    for r in s:
        read += 1; ts = r['timestamp']; mn = ts if mn is None or ts < mn else mn; mx = ts if mx is None or ts > mx else mx
        d.execute("select 1 from sui.raw_pool_snapshots where source_id='legacy_neon' and protocol=%s and symbol=%s and fetched_at=%s", (r.get('protocol'), r['symbol'], ts))
        if d.fetchone(): continue
        d.execute("""insert into sui.raw_pool_snapshots (run_id, source_id, fetched_at, protocol, symbol, total_supply, total_supply_usd, total_borrows,
                     total_borrows_usd, available_liquidity_usd, supply_apy, borrow_apy, utilization, ltv, liquidation_threshold, payload)
                     values (%s,'legacy_neon',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                  (run_id, ts, r.get('protocol'), r['symbol'], r.get('totalSupply'), r.get('totalSupplyUsd'), r.get('totalBorrows'), r.get('totalBorrowsUsd'),
                   r.get('availableLiquidityUsd'), r.get('supplyApy'), r.get('borrowApy'), r.get('utilization'),
                   (r.get('ltv') or 0) * (100 if (r.get('ltv') or 0) <= 1 else 1) if r.get('ltv') is not None else None,
                   (r.get('liquidationThreshold') or 0) * (100 if (r.get('liquidationThreshold') or 0) <= 1 else 1) if r.get('liquidationThreshold') is not None else None,
                   json.dumps(dict(r), default=str)))
        written += 1
        if written % 500 == 0: dst.commit()
    dst.commit(); close(iid, read, written, mn, mx); print(f'PoolSnapshot: read {read}, written {written}, {mn} .. {mx}')
except Exception as e:
    dst.rollback(); close(iid, read, written, mn, mx, 'error', str(e)); raise
s.close(); s = src.cursor(cursor_factory=psycopg2.extras.RealDictCursor, name='legacy2')

# ---- LiquidationEvent -> raw_liquidation_events
iid = receipt('LiquidationEvent', 'sui.raw_liquidation_events'); read = written = 0; mn = mx = None
try:
    s.execute('select * from "LiquidationEvent" where "timestamp" >= %s order by "timestamp"', (a.since,))
    for r in s:
        read += 1; ts = r['timestamp']; mn = ts if mn is None or ts < mn else mn; mx = ts if mx is None or ts > mx else mx
        d.execute("""insert into sui.raw_liquidation_events (event_id, run_id, source_id, fetched_at, protocol, tx_digest, ts, liquidator, borrower,
                     collateral_asset, collateral_amount, collateral_price, collateral_usd, debt_asset, debt_amount, debt_price, debt_usd, treasury_amount,
                     gas_used_mist, gas_usd, payload)
                     values (%s,%s,'legacy_neon',now(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) on conflict (event_id) do nothing""",
                  (r['id'], run_id, r.get('protocol'), r.get('txDigest'), ts, r.get('liquidator'), r.get('borrower'),
                   r.get('collateralAsset'), r.get('collateralAmount'), r.get('collateralPrice'), r.get('collateralUsd'),
                   r.get('debtAsset'), r.get('debtAmount'), r.get('debtPrice'), r.get('debtUsd'), r.get('treasuryAmount'),
                   str(r['gasUsedMist']) if r.get('gasUsedMist') is not None else None, r.get('gasUsd'), json.dumps(dict(r), default=str)))
        written += d.rowcount
        if read % 500 == 0: dst.commit()
    dst.commit(); close(iid, read, written, mn, mx); print(f'LiquidationEvent: read {read}, written {written}, {mn} .. {mx}')
except Exception as e:
    dst.rollback(); close(iid, read, written, mn, mx, 'error', str(e)); raise
s.close(); s = src.cursor(cursor_factory=psycopg2.extras.RealDictCursor, name='legacy3')

# ---- DefillamaTvl -> raw_defillama_tvl (versioned by this run)
iid = receipt('DefillamaTvl', 'sui.raw_defillama_tvl'); read = written = 0; mn = mx = None
try:
    s.execute('select * from "DefillamaTvl" order by "date"')
    for r in s:
        read += 1
        d.execute("""insert into sui.raw_defillama_tvl (protocol, slug, day, tvl_usd, fetched_at, run_id) values (%s,%s,%s,%s,now(),%s)
                     on conflict (protocol, day, run_id) do nothing""", (r.get('protocol'), 'legacy', r['date'], r.get('tvl'), run_id))
        written += d.rowcount
    dst.commit(); close(iid, read, written, None, None); print(f'DefillamaTvl: read {read}, written {written}')
except Exception as e:
    dst.rollback(); close(iid, read, written, None, None, 'error', str(e)); raise
d.execute("update ops.sync_runs set finished_at=now(), status='ok' where run_id=%s", (run_id,)); dst.commit()
print('legacy import complete; receipts in ops.legacy_imports')
