"""Hourly Aave ingest: every reserve of every Aave v3 market on 20 chains (api.v3.aave.com) and
every Aave v4 reserve on the Ethereum hub (api.v4.aave.com). Append-only snapshots with run logs.
No JSON payload is stored: the API is already flat and the columns carry everything the marts use.

  DATABASE_URL=... python scripts/ingest_aave.py [--v3] [--v4]
"""
import argparse, os, sys, json, time, base64, datetime as dt
import requests, psycopg2, psycopg2.extras

V3 = 'https://api.v3.aave.com/graphql'; V4 = 'https://api.v4.aave.com/graphql'
CHAINS = {1: 'Ethereum', 42161: 'Arbitrum', 43114: 'Avalanche', 8453: 'Base', 10: 'Optimism', 137: 'Polygon', 56: 'BNB Chain', 100: 'Gnosis', 59144: 'Linea',
          9745: 'Plasma', 5000: 'Mantle', 534352: 'Scroll', 146: 'Sonic', 42220: 'Celo', 324: 'zkSync', 57073: 'Ink', 1088: 'Metis', 1868: 'Soneium', 4326: 'MegaETH', 196: 'X Layer'}
V4_CHAINS = [1]
p = argparse.ArgumentParser(); p.add_argument('--v3', action='store_true'); p.add_argument('--v4', action='store_true'); a = p.parse_args()
ALL = not (a.v3 or a.v4)
url = os.environ.get('DATABASE_URL_DIRECT') or os.environ.get('DATABASE_URL')
if not url: sys.exit('DATABASE_URL not set')
conn = psycopg2.connect(url); cur = conn.cursor()
RUNNER = 'github-actions' if os.environ.get('GITHUB_ACTIONS') else 'local'

def gql(endpoint, query, variables, tries=4):
    last = None
    for i in range(tries):
        r = requests.post(endpoint, json={'query': query, 'variables': variables}, timeout=120); last = r
        if r.status_code == 200 and 'errors' not in r.json(): return r.json()['data']
        time.sleep(2 * (i + 1))
    raise RuntimeError(f'{endpoint} failed: {last.status_code} {last.text[:200]}')
def f(v):
    try: return None if v is None else float(v)
    except (TypeError, ValueError): return None
def g(d, *path):
    for k in path:
        d = (d or {}).get(k)
    return d
def open_run(job): cur.execute("insert into ops.sync_runs (job, product, runner, notes) values (%s,'aave',%s,'{}') returning run_id", (job, RUNNER)); conn.commit(); return cur.fetchone()[0]
def close_run(run_id, status, rows, error=None): cur.execute("update ops.sync_runs set finished_at=now(), status=%s, rows_written=%s, error=%s where run_id=%s", (status, rows, error, run_id)); conn.commit()
def freshness(source, ok):
    cur.execute("""insert into ops.source_freshness (product, source_id, expected_hours, last_seen_at, last_status) values ('aave',%s,3,now(),%s)
                   on conflict (product, source_id) do update set last_seen_at=now(), last_status=excluded.last_status, updated_at=now()""", (source, 'ok' if ok else 'broken')); conn.commit()

RES_SQL = """insert into aave.raw_reserve_snapshots (run_id, source_id, fetched_at, version, chain_id, chain_name, market_name, market_address, underlying_address, symbol, decimals,
  price_usd, supply_amount, supply_usd, supply_apy, liquidation_threshold, borrow_amount, borrow_usd, borrow_apy, utilization) values %s"""
failures = []
if ALL or a.v3:
    run = open_run('aave.v3_reserves'); n = 0; fetched = dt.datetime.now(dt.timezone.utc)
    try:
        q = """query($c:[ChainId!]!){ markets(request:{chainIds:$c}){ name address chain{name chainId} totalMarketSize
          reserves{ underlyingToken{address symbol decimals} size{usdPerToken amount{value} usd} supplyInfo{apy{value} total{value} liquidationThreshold{value}}
                    borrowInfo{apy{value} total{amount{value} usd} utilizationRate{value}} } } }"""
        markets = gql(V3, q, {'c': list(CHAINS)})['markets']; rows = []; mrows = []
        for m in markets:
            cid = m['chain']['chainId']; addr = (m.get('address') or '').lower()
            mrows.append((run, 'aave_v3_api', fetched, 'v3', cid, m['chain'].get('name'), m.get('name'), addr, f(m.get('totalMarketSize')), len(m.get('reserves') or [])))
            for r in m.get('reserves') or []:
                t = r.get('underlyingToken') or {}
                rows.append((run, 'aave_v3_api', fetched, 'v3', cid, m['chain'].get('name'), m.get('name'), addr, (t.get('address') or '').lower(), t.get('symbol'), t.get('decimals'),
                             f(g(r, 'size', 'usdPerToken')), f(g(r, 'supplyInfo', 'total', 'value')), f(g(r, 'size', 'usd')), f(g(r, 'supplyInfo', 'apy', 'value')), f(g(r, 'supplyInfo', 'liquidationThreshold', 'value')),
                             f(g(r, 'borrowInfo', 'total', 'amount', 'value')), f(g(r, 'borrowInfo', 'total', 'usd')), f(g(r, 'borrowInfo', 'apy', 'value')), f(g(r, 'borrowInfo', 'utilizationRate', 'value'))))
        psycopg2.extras.execute_values(cur, RES_SQL, rows, page_size=500)
        psycopg2.extras.execute_values(cur, "insert into aave.raw_market_snapshots (run_id, source_id, fetched_at, version, chain_id, chain_name, market_name, market_address, total_market_size_usd, reserve_count) values %s", mrows)
        conn.commit(); n = len(rows); close_run(run, 'ok', n); freshness('aave_v3_api', True)
        print(f'[aave v3] {len(markets)} markets, {n} reserves, ${sum(x[8] or 0 for x in mrows)/1e9:.1f}B market size')
    except Exception as e:
        conn.rollback(); close_run(run, 'error', n, str(e)); freshness('aave_v3_api', False); failures.append(f'v3: {e}'); print('[aave v3] FAILED', e)

if ALL or a.v4:
    run = open_run('aave.v4_reserves'); n = 0; fetched = dt.datetime.now(dt.timezone.utc)
    try:
        q = """query($c:[Int!]!){ reserves(request:{query:{chainIds:$c}, filter: ALL, orderBy:{supplyAvailable: DESC}}){ id chain{name chainId} asset{underlying{address info{name symbol decimals} }}
          summary{ supplied{amount{value} exchange{value}} borrowed{amount{value} exchange{value}} supplyApy{value} borrowApy{value} } } }"""
        rows = []
        for cid in V4_CHAINS:
            for r in gql(V4, q, {'c': [cid]})['reserves']:
                info = g(r, 'asset', 'underlying', 'info') or {}; s = r.get('summary') or {}
                # v4 is hub-and-spoke: the same underlying exists once per spoke. The reserve id decodes to "chainId::spokeAddress::assetId".
                try: spoke = base64.b64decode(r.get('id') or '').decode().split('::')[1].lower()
                except Exception: spoke = None
                sa, su = f(g(s, 'supplied', 'amount', 'value')), f(g(s, 'supplied', 'exchange', 'value')); ba, bu = f(g(s, 'borrowed', 'amount', 'value')), f(g(s, 'borrowed', 'exchange', 'value'))
                rows.append((run, 'aave_v4_api', fetched, 'v4', cid, (r.get('chain') or {}).get('name'), 'AaveV4Spoke', spoke, ((g(r, 'asset', 'underlying', 'address') or r.get('id') or '')).lower(), info.get('symbol'), info.get('decimals'),
                             (su / sa) if sa else None, sa, su, f(g(s, 'supplyApy', 'value')), None, ba, bu, f(g(s, 'borrowApy', 'value')), None))   # spoke-level utilization is not meaningful: liquidity sits in the hub
        psycopg2.extras.execute_values(cur, RES_SQL, rows, page_size=500); conn.commit(); n = len(rows); close_run(run, 'ok', n); freshness('aave_v4_api', True)
        print(f'[aave v4] {n} reserves, ${sum(x[13] or 0 for x in rows)/1e9:.2f}B supplied')
    except Exception as e:
        conn.rollback(); close_run(run, 'error', n, str(e)); freshness('aave_v4_api', False); failures.append(f'v4: {e}'); print('[aave v4] FAILED', e)
sys.exit(1 if failures else 0)
