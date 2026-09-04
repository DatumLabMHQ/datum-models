"""Import the months of Sui history held in the existing SuiLending Neon database (Prisma tables
PoolSnapshot, LiquidationEvent, DefillamaTvl) into the platform's raw layer.

Read-only against the legacy database. Rows are appended with source_id = 'legacy_neon' and a
receipt in ops.legacy_imports. Re-runnable: pool snapshots skip keys already imported,
liquidation events dedupe on event_id and on tx digests the platform already decoded itself,
DefiLlama rows are versioned by run (staging keeps the latest fetch per day).
Legacy timestamps are naive and were written in UTC.

  LEGACY_SUI_DATABASE_URL=postgresql://...   python scripts/import_legacy_sui.py [--since YYYY-MM-DD]
"""
import argparse, os, sys, json, datetime as dt
import psycopg2, psycopg2.extras

p = argparse.ArgumentParser(); p.add_argument('--since', default='2025-01-01'); a = p.parse_args()
src_url = (os.environ.get('LEGACY_SUI_DATABASE_URL') or '').replace('&channel_binding=require', '')
dst_url = os.environ.get('DATABASE_URL_DIRECT') or os.environ.get('DATABASE_URL')
if not src_url:
    print('LEGACY_SUI_DATABASE_URL not set; skipping legacy Sui import'); sys.exit(0)
if not dst_url: sys.exit('DATABASE_URL must be set')
src = psycopg2.connect(src_url); src.set_session(readonly=True); dst = psycopg2.connect(dst_url)
d = dst.cursor(); d.execute("set timezone = 'UTC'"); dst.commit()
UTC = dt.timezone.utc
def utc(ts): return ts.replace(tzinfo=UTC) if ts is not None and ts.tzinfo is None else ts
def pct(v): return None if v is None else v * (100 if v <= 1 else 1)

def receipt(source_table, target_table):
    d.execute("insert into ops.legacy_imports (product, source_db, source_table, target_table) values ('sui','suilending-neon',%s,%s) returning import_id", (source_table, target_table))
    dst.commit(); return d.fetchone()[0]
def close(iid, read, written, mn, mx, status='ok', error=None):
    d.execute("update ops.legacy_imports set finished_at=now(), rows_read=%s, rows_written=%s, min_ts=%s, max_ts=%s, status=%s, error=%s where import_id=%s", (read, written, mn, mx, status, error, iid)); dst.commit()
def stream(sql, params=(), name='legacy'):
    s = src.cursor(cursor_factory=psycopg2.extras.RealDictCursor, name=name); s.itersize = 5000; s.execute(sql, params); return s

d.execute("insert into ops.sync_runs (job, product, runner, notes) values ('sui.import_legacy','sui','local','{}') returning run_id"); run_id = d.fetchone()[0]; dst.commit()
failures = []

# ---- PoolSnapshot -> raw_pool_snapshots
iid = receipt('PoolSnapshot', 'sui.raw_pool_snapshots'); read = written = 0; mn = mx = None
try:
    d.execute("select protocol, symbol, fetched_at from sui.raw_pool_snapshots where source_id='legacy_neon'"); have = set(d.fetchall())
    batch = []
    def flush():
        global written
        if not batch: return
        psycopg2.extras.execute_values(d, """insert into sui.raw_pool_snapshots (run_id, source_id, fetched_at, protocol, symbol, total_supply, total_supply_usd, total_borrows,
            total_borrows_usd, available_liquidity_usd, supply_apy, borrow_apy, utilization, ltv, liquidation_threshold, payload) values %s""", batch, page_size=2000)
        dst.commit(); written += len(batch); batch.clear()
    for r in stream('select * from "PoolSnapshot" where "timestamp" >= %s order by "timestamp"', (a.since,)):
        read += 1; ts = utc(r['timestamp']); mn = ts if mn is None or ts < mn else mn; mx = ts if mx is None or ts > mx else mx
        key = (r.get('protocol'), r['symbol'], ts)
        if key in have: continue
        have.add(key)
        batch.append((run_id, 'legacy_neon', ts, r.get('protocol'), r['symbol'], r.get('totalSupply'), r.get('totalSupplyUsd'), r.get('totalBorrows'), r.get('totalBorrowsUsd'),
                      r.get('availableLiquidityUsd'), r.get('supplyApy'), r.get('borrowApy'), r.get('utilization'), pct(r.get('ltv')), pct(r.get('liquidationThreshold')),
                      json.dumps(dict(r), default=str)))
        if len(batch) >= 2000: flush()
    flush(); close(iid, read, written, mn, mx); print(f'PoolSnapshot: read {read}, written {written}, {mn} .. {mx}')
except Exception as e:
    dst.rollback(); close(iid, read, written, mn, mx, 'error', str(e)); failures.append(f'PoolSnapshot: {e}'); print('PoolSnapshot FAILED', e)

# ---- LiquidationEvent -> raw_liquidation_events
iid = receipt('LiquidationEvent', 'sui.raw_liquidation_events'); read = written = skipped = 0; mn = mx = None
try:
    d.execute("select tx_digest from sui.raw_liquidation_events where source_id <> 'legacy_neon'"); have_tx = {t for (t,) in d.fetchall()}
    batch = []
    def flush2():
        global written
        if not batch: return
        before = written
        psycopg2.extras.execute_values(d, """insert into sui.raw_liquidation_events (event_id, run_id, source_id, fetched_at, protocol, tx_digest, ts, liquidator, borrower,
            collateral_asset, collateral_amount, collateral_price, collateral_usd, debt_asset, debt_amount, debt_price, debt_usd, treasury_amount, gas_used_mist, gas_usd, payload)
            values %s on conflict (event_id) do nothing""", batch, page_size=2000)
        written += d.rowcount if d.rowcount is not None and d.rowcount >= 0 else len(batch); dst.commit(); batch.clear()
    for r in stream('select * from "LiquidationEvent" where "timestamp" >= %s order by "timestamp"', (a.since,), name='legacy2'):
        read += 1; ts = utc(r['timestamp']); mn = ts if mn is None or ts < mn else mn; mx = ts if mx is None or ts > mx else mx
        if r.get('txDigest') in have_tx: skipped += 1; continue
        batch.append((r['id'], run_id, 'legacy_neon', dt.datetime.now(UTC), r.get('protocol'), r.get('txDigest'), ts, r.get('liquidator'), r.get('borrower'),
                      r.get('collateralAsset'), r.get('collateralAmount'), r.get('collateralPrice'), r.get('collateralUsd'), r.get('debtAsset'), r.get('debtAmount'),
                      r.get('debtPrice'), r.get('debtUsd'), r.get('treasuryAmount'), str(r['gasUsedMist']) if r.get('gasUsedMist') is not None else None, r.get('gasUsd'),
                      json.dumps(dict(r), default=str)))
        if len(batch) >= 2000: flush2()
    flush2(); close(iid, read, written, mn, mx); print(f'LiquidationEvent: read {read}, written {written}, skipped {skipped} already decoded by the platform, {mn} .. {mx}')
except Exception as e:
    dst.rollback(); close(iid, read, written, mn, mx, 'error', str(e)); failures.append(f'LiquidationEvent: {e}'); print('LiquidationEvent FAILED', e)

# ---- DefillamaTvl -> raw_defillama_tvl (versioned by this run; staging picks the latest fetch per day)
iid = receipt('DefillamaTvl', 'sui.raw_defillama_tvl'); read = written = 0
try:
    rows = [(r.get('protocol'), 'legacy', r['date'], r.get('tvlUsd'), dt.datetime.now(UTC), run_id) for r in stream('select * from "DefillamaTvl" order by "date"', name='legacy3')]
    read = len(rows)
    psycopg2.extras.execute_values(d, "insert into sui.raw_defillama_tvl (protocol, slug, day, tvl_usd, fetched_at, run_id) values %s on conflict (protocol, day, run_id) do nothing", rows, page_size=2000)
    written = d.rowcount; dst.commit(); close(iid, read, written, None, None); print(f'DefillamaTvl: read {read}, written {written}')
except Exception as e:
    dst.rollback(); close(iid, read, written, None, None, 'error', str(e)); failures.append(f'DefillamaTvl: {e}'); print('DefillamaTvl FAILED', e)

d.execute("update ops.sync_runs set finished_at=now(), status=%s, error=%s where run_id=%s", ('ok' if not failures else 'error', '; '.join(failures) or None, run_id)); dst.commit()
print('legacy import complete; receipts in ops.legacy_imports'); sys.exit(1 if failures else 0)
