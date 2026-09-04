"""Incremental, read-only copy of the RWA terminal's history from the legacy Neon database into
the platform's raw layer (schema rwa). Safe to run every hour: each history table has a cursor
(max ts already imported) and rows are inserted with ON CONFLICT DO NOTHING. Dimension tables
are refreshed in full. Every run writes ops.sync_runs and ops.legacy_imports receipts.

  LEGACY_RWA_DATABASE_URL=... DATABASE_URL=...  python scripts/import_legacy_rwa.py [--full]
"""
import argparse, os, sys, io, csv
import psycopg2, psycopg2.extras

p = argparse.ArgumentParser(); p.add_argument('--full', action='store_true', help='ignore cursors and re-scan everything (still dedupes)'); a = p.parse_args()
src_url = (os.environ.get('LEGACY_RWA_DATABASE_URL') or '').replace('&channel_binding=require', '')
dst_url = os.environ.get('DATABASE_URL_DIRECT') or os.environ.get('DATABASE_URL')
if not src_url or not dst_url: sys.exit('LEGACY_RWA_DATABASE_URL and DATABASE_URL must be set')
src = psycopg2.connect(src_url); dst = psycopg2.connect(dst_url); d = dst.cursor()

HISTORY = [  # (source table, target table, ts column, columns)
    ('market_history', 'rwa.raw_market_history', 'ts', ['ts','rwa_aum','stablecoin_aum','total_aum','horizon_supplied_usd','holders','active_addresses','actions','issuers']),
    ('reserve_state_history', 'rwa.raw_reserve_state_history', 'ts', ['ts','reserve','symbol','supplied_usd','supply_apy_pct','ltv_pct','liq_threshold_pct','oracle_price','nav','borrowed_usd','utilization_pct','borrow_apy_pct']),
    ('horizon_holder_history', 'rwa.raw_horizon_holder_history', 'ts', ['ts','reserve','symbol','asset_class','holders']),
    ('morpho_market_history', 'rwa.raw_morpho_market_history', 'ts', ['ts','market_id','collateral_symbol','loan_symbol','asset_class','lltv','collateral_usd','borrow_usd','utilization','borrow_apy']),
    ('morpho_risk_history', 'rwa.raw_morpho_risk_history', 'ts', ['ts','total_collateral_usd','total_borrow_usd','avg_lltv','market_count','borrowers','min_health_factor','hf_at_risk','hf_tight','hf_moderate','hf_safe']),
    ('asset_aum_history', 'rwa.raw_asset_aum_history', 'ts', ['asset_id','ts','aum','source']),
    ('asset_nav_history', 'rwa.raw_asset_nav_history', 'ts', ['asset_id','ts','nav','source','updated_at_onchain','round_id','is_stale']),
    ('token_supply_history', 'rwa.raw_token_supply_history', 'ts', ['token_id','ts','total_supply','circulating']),
    ('token_top_holders', 'rwa.raw_token_top_holders', 'fetched_at', ['address','fetched_at','symbol','holders']),
]
DIMS = [
    ('asset', 'rwa.raw_asset', ['asset_id','name','display_ticker','issuer_id','investment_manager_id','platform_id','transfer_agent_id','isin','redemption_terms','settlement_cycle','rating','created_at','updated_at']),
    ('token', 'rwa.raw_token', ['token_id','asset_id','chain_id','contract_address','token_standard','decimals','symbol','distributed_or_represented','is_wrapper','wraps_token_id','oracle_provider_id','nav_aggregator_addr','nav_heartbeat_secs','created_at']),
    ('issuer', 'rwa.raw_issuer', ['issuer_id','name','legal_name','type','website','data_source_url','notes','created_at','updated_at']),
]

d.execute("insert into ops.sync_runs (job, product, runner, notes) values ('rwa.import_legacy','rwa',%s,'{}') returning run_id",
          ('github-actions' if os.environ.get('GITHUB_ACTIONS') else 'local',)); run_id = d.fetchone()[0]; dst.commit()
total_written = 0; failures = []

def receipt(st, tt):
    d.execute("insert into ops.legacy_imports (product, source_db, source_table, target_table) values ('rwa','rwa-terminal-neon',%s,%s) returning import_id", (st, tt)); dst.commit(); return d.fetchone()[0]
def close(iid, read, written, mn, mx, status='ok', error=None):
    d.execute("update ops.legacy_imports set finished_at=now(), rows_read=%s, rows_written=%s, min_ts=%s, max_ts=%s, status=%s, error=%s where import_id=%s", (read, written, mn, mx, status, error, iid)); dst.commit()

for st, tt, tscol, cols in HISTORY:
    iid = receipt(st, tt); read = written = 0; mn = mx = None
    try:
        d.execute(f"select max({tscol}) from {tt}"); cursor = None if a.full else d.fetchone()[0]
        s = src.cursor(name=f'imp_{st}', cursor_factory=psycopg2.extras.DictCursor); s.itersize = 5000
        q = f'select {", ".join(cols)} from {st}' + (f' where {tscol} > %s' if cursor else '') + f' order by {tscol}'
        s.execute(q, (cursor,) if cursor else None)
        batch = []
        def flush():
            global written
            if not batch: return
            psycopg2.extras.execute_values(d,
                f"insert into {tt} ({', '.join(cols)}, run_id) values %s on conflict do nothing",
                [tuple(list(r) + [run_id]) for r in batch], template=None, page_size=5000)
            written += d.rowcount if d.rowcount is not None and d.rowcount >= 0 else 0
            dst.commit(); batch.clear()
        for r in s:
            read += 1; ts = r[tscol]; mn = ts if mn is None or ts < mn else mn; mx = ts if mx is None or ts > mx else mx
            batch.append([psycopg2.extras.Json(v) if isinstance(v, (dict, list)) else v for v in r])
            if len(batch) >= 5000: flush()
        flush(); s.close(); close(iid, read, written, mn, mx); total_written += written
        print(f'{st}: read {read} new rows since cursor, written {written}' + (f' ({mn:%Y-%m-%d} .. {mx:%Y-%m-%d})' if mn else ''))
    except Exception as e:
        dst.rollback(); close(iid, read, written, mn, mx, 'error', str(e)); failures.append(f'{st}: {e}'); print(f'{st} FAILED: {e}')

for st, tt, cols in DIMS:
    iid = receipt(st, tt)
    try:
        s = src.cursor(cursor_factory=psycopg2.extras.DictCursor); s.execute(f'select {", ".join(cols)} from {st}'); rows = s.fetchall(); s.close()
        sets = ', '.join(f'{c}=excluded.{c}' for c in cols[1:])
        psycopg2.extras.execute_values(d, f"insert into {tt} ({', '.join(cols)}, imported_at) values %s on conflict ({cols[0]}) do update set {sets}, imported_at=now()",
                                       [tuple(psycopg2.extras.Json(v) if isinstance(v, (dict, list)) else v for v in r) for r in rows],
                                       template='(' + ','.join(['%s']*len(cols)) + ', now())')
        dst.commit(); close(iid, len(rows), len(rows), None, None); print(f'{st}: refreshed {len(rows)} rows')
    except Exception as e:
        dst.rollback(); close(iid, 0, 0, None, None, 'error', str(e)); failures.append(f'{st}: {e}'); print(f'{st} FAILED: {e}')

status = 'ok' if not failures else 'error'
d.execute("update ops.sync_runs set finished_at=now(), status=%s, rows_written=%s, error=%s where run_id=%s", (status, total_written, '; '.join(failures) or None, run_id))
d.execute("""insert into ops.source_freshness (product, source_id, expected_hours, last_seen_at, last_status) values ('rwa','legacy_neon_rwa',2,now(),%s)
             on conflict (product, source_id) do update set last_seen_at=now(), last_status=excluded.last_status, updated_at=now()""", (('ok' if not failures else 'broken'),))
dst.commit(); print(f'rwa legacy import {status}: {total_written} new rows'); sys.exit(1 if failures else 0)
