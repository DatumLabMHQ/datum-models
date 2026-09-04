"""Hourly Centrifuge ingest from api.centrifuge.io: active pools with their tokens and per-chain
instances, recent investor transactions (deduped), and the API's own per-token history
(tokenSnapshots), which backfills supply and price before our first run. Append-only with run logs.

  DATABASE_URL=... python scripts/ingest_centrifuge.py [--pools] [--transactions] [--history]
"""
import argparse, os, sys, json, time, hashlib, datetime as dt
import requests, psycopg2, psycopg2.extras

API = 'https://api.centrifuge.io/'
p = argparse.ArgumentParser()
for x in ('pools', 'transactions', 'history'): p.add_argument(f'--{x}', action='store_true')
a = p.parse_args(); ALL = not (a.pools or a.transactions or a.history)
url = os.environ.get('DATABASE_URL_DIRECT') or os.environ.get('DATABASE_URL')
if not url: sys.exit('DATABASE_URL not set')
conn = psycopg2.connect(url); cur = conn.cursor()
RUNNER = 'github-actions' if os.environ.get('GITHUB_ACTIONS') else 'local'

def gql(query, variables, tries=4):
    last = None
    for i in range(tries):
        try:
            r = requests.post(API, json={'query': query, 'variables': variables}, timeout=90); last = r
            if r.status_code == 200 and 'errors' not in r.json(): return r.json()['data']
        except requests.RequestException as e:
            last = e
        time.sleep(3 * (i + 1))
    raise RuntimeError(f'Centrifuge API failed: {getattr(last, "status_code", last)} {getattr(last, "text", "")[:200]}')
def num(v):
    try: return None if v in (None, '') else float(v)
    except (TypeError, ValueError): return None
def ts_ms(v):
    if v in (None, ''): return None
    x = float(v); x = x / 1000 if x > 1e11 else x
    return dt.datetime.fromtimestamp(x, dt.timezone.utc)
def open_run(job): cur.execute("insert into ops.sync_runs (job, product, runner, notes) values (%s,'centrifuge',%s,'{}') returning run_id", (job, RUNNER)); conn.commit(); return cur.fetchone()[0]
def close_run(run_id, status, rows, error=None): cur.execute("update ops.sync_runs set finished_at=now(), status=%s, rows_written=%s, error=%s where run_id=%s", (status, rows, error, run_id)); conn.commit()
def freshness(ok):
    cur.execute("""insert into ops.source_freshness (product, source_id, expected_hours, last_seen_at, last_status) values ('centrifuge','centrifuge_api',3,now(),%s)
                   on conflict (product, source_id) do update set last_seen_at=now(), last_status=excluded.last_status, updated_at=now()""", ('ok' if ok else 'broken',)); conn.commit()

POOLS_Q = """query($limit:Int!){ pools(limit:$limit, where:{isActive:true}){ items{ id name isActive currency decimals metadata
  tokens{ items{ id symbol name decimals isActive totalIssuance tokenPrice tokenInstances{ items{ centrifugeId address isActive totalIssuance tokenPrice blockchain{ name chainId centrifugeId } } } } } } } }"""
TX_Q = """query($limit:Int!){ investorTransactions(limit:$limit, orderBy:"createdAt", orderDirection:"desc",
  where:{ type_in:[SYNC_DEPOSIT, DEPOSIT_CLAIMED, DEPOSIT_REQUEST_EXECUTED, SYNC_REDEEM, REDEEM_CLAIMED, REDEEM_REQUEST_EXECUTED] }){
  items{ createdAtTxHash poolId tokenId type account centrifugeId currencyAmount tokenAmount tokenPrice createdAt blockchain{ name chainId centrifugeId } } } }"""
HIST_Q = """query($id:String!, $limit:Int!){ tokenSnapshots(limit:$limit, orderBy:"timestamp", orderDirection:"desc", where:{ id:$id }){ items{ timestamp totalIssuance tokenPrice } } }"""

failures = []; token_ids = []
if ALL or a.pools or a.history:
    run = open_run('centrifuge.pools'); n = 0; fetched = dt.datetime.now(dt.timezone.utc)
    try:
        pools = gql(POOLS_Q, {'limit': 200})['pools']['items']; prow = []; trow = []
        for pl in pools:
            toks = (pl.get('tokens') or {}).get('items') or []
            prow.append((run, fetched, pl['id'], pl.get('name'), pl.get('isActive'), pl.get('currency'), pl.get('decimals'), pl.get('metadata'), len(toks)))
            for t in toks:
                token_ids.append(t['id']); inst = (t.get('tokenInstances') or {}).get('items') or []
                trow.append((run, fetched, pl['id'], t['id'], t.get('symbol'), t.get('name'), t.get('decimals'), t.get('isActive'), num(t.get('totalIssuance')), num(t.get('tokenPrice')), json.dumps(inst)))
        if ALL or a.pools:
            psycopg2.extras.execute_values(cur, "insert into centrifuge.raw_pool_snapshots (run_id, fetched_at, pool_id, name, is_active, currency, decimals, metadata, token_count) values %s", prow)
            psycopg2.extras.execute_values(cur, "insert into centrifuge.raw_token_snapshots (run_id, fetched_at, pool_id, token_id, symbol, name, decimals, is_active, total_issuance, token_price, instances) values %s", trow)
            conn.commit(); n = len(prow) + len(trow)
        close_run(run, 'ok', n); freshness(True); print(f'[centrifuge] {len(prow)} active pools, {len(trow)} tokens')
    except Exception as e:
        conn.rollback(); close_run(run, 'error', n, str(e)); freshness(False); failures.append(f'pools: {e}'); print('[centrifuge] pools FAILED', e)

if ALL or a.transactions:
    run = open_run('centrifuge.transactions'); n = 0; fetched = dt.datetime.now(dt.timezone.utc)
    try:
        rows = []
        for t in gql(TX_Q, {'limit': 1000})['investorTransactions']['items']:
            key = hashlib.md5('|'.join(str(t.get(k)) for k in ('createdAtTxHash', 'type', 'account', 'tokenId', 'tokenAmount', 'currencyAmount', 'createdAt')).encode()).hexdigest()
            b = t.get('blockchain') or {}
            rows.append((key, run, fetched, t.get('createdAtTxHash'), t.get('type'), t.get('account'), t.get('poolId'), t.get('tokenId'), t.get('centrifugeId'), b.get('chainId'), b.get('name'),
                         num(t.get('currencyAmount')), num(t.get('tokenAmount')), num(t.get('tokenPrice')), ts_ms(t.get('createdAt'))))
        cur.execute('select count(*) from centrifuge.raw_investor_transactions'); before = cur.fetchone()[0]
        psycopg2.extras.execute_values(cur, """insert into centrifuge.raw_investor_transactions (event_key, run_id, fetched_at, tx_hash, type, account, pool_id, token_id, centrifuge_id, chain_id, chain_name,
            currency_amount, token_amount, token_price, created_at) values %s on conflict (event_key) do nothing""", rows, page_size=500)
        cur.execute('select count(*) from centrifuge.raw_investor_transactions'); n = cur.fetchone()[0] - before; conn.commit(); close_run(run, 'ok', n); print(f'[centrifuge] transactions: {len(rows)} fetched, {n} new')
    except Exception as e:
        conn.rollback(); close_run(run, 'error', n, str(e)); failures.append(f'transactions: {e}'); print('[centrifuge] transactions FAILED', e)

if ALL or a.history:
    run = open_run('centrifuge.token_history'); n = 0; fetched = dt.datetime.now(dt.timezone.utc)
    try:
        for tid in token_ids:
            items = gql(HIST_Q, {'id': tid, 'limit': 1000})['tokenSnapshots']['items']
            rows = [(tid, ts_ms(i.get('timestamp')), num(i.get('totalIssuance')), num(i.get('tokenPrice')), fetched, run) for i in items if i.get('timestamp')]
            if rows:
                psycopg2.extras.execute_values(cur, "insert into centrifuge.raw_token_history (token_id, ts, total_issuance, token_price, fetched_at, run_id) values %s on conflict (token_id, ts) do nothing", rows, page_size=1000)
                n += cur.rowcount; conn.commit()
        close_run(run, 'ok', n); print(f'[centrifuge] token history: {n} new points across {len(token_ids)} tokens')
    except Exception as e:
        conn.rollback(); close_run(run, 'error', n, str(e)); failures.append(f'history: {e}'); print('[centrifuge] history FAILED', e)
sys.exit(1 if failures else 0)
