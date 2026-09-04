"""Hourly Morpho ingest: snapshot every market and vault on every chain from the Morpho GraphQL
API and refresh the curator registry. Listed markets/vaults every hour; every market/vault once a day
(00:xx UTC) or with --full. Append-only raw rows with run logs; no cursors needed.

  DATABASE_URL=... python scripts/ingest_morpho.py [--markets] [--vaults] [--curators] [--full]
"""
import argparse, os, sys, json, time, datetime as dt
import requests, psycopg2, psycopg2.extras

API = 'https://blue-api.morpho.org/graphql'
NON_CHAIN_KEYS = ('borrowed', 'staking', 'pool2', 'vesting', 'offers', 'treasury')
p = argparse.ArgumentParser()
for x in ('markets', 'vaults', 'curators', 'full'): p.add_argument(f'--{x}', action='store_true')
a = p.parse_args()
ALL = not any([a.markets, a.vaults, a.curators])
# Unlisted markets and vaults are mostly dust and fake-price entries: snapshot them once a day (00:xx UTC) or with --full;
# listed ones every hour. The full JSON payload is stored only in the daily sweep, for listed rows, to keep Neon small.
FULL = a.full or dt.datetime.now(dt.timezone.utc).hour == 0
url = os.environ.get('DATABASE_URL_DIRECT') or os.environ.get('DATABASE_URL')
if not url: sys.exit('DATABASE_URL not set')
conn = psycopg2.connect(url); cur = conn.cursor()
RUNNER = 'github-actions' if os.environ.get('GITHUB_ACTIONS') else 'local'

def gql(query, variables=None, tries=4):
    last = None
    for i in range(tries):
        r = requests.post(API, json={'query': query, 'variables': variables or {}}, timeout=60); last = r
        if r.status_code == 200 and 'errors' not in r.json(): return r.json()['data']
        time.sleep(1.5 * (i + 1))
    raise RuntimeError(f'Morpho GraphQL failed: {last.status_code} {last.text[:200]}')
def open_run(job, notes=None):
    cur.execute("insert into ops.sync_runs (job, product, runner, notes) values (%s,'morpho',%s,%s) returning run_id", (job, RUNNER, json.dumps(notes or {}))); conn.commit(); return cur.fetchone()[0]
def close_run(run_id, status, rows, error=None):
    cur.execute("update ops.sync_runs set finished_at=now(), status=%s, rows_written=%s, error=%s where run_id=%s", (status, rows, error, run_id)); conn.commit()
def freshness(source, ok, hours):
    cur.execute("""insert into ops.source_freshness (product, source_id, expected_hours, last_seen_at, last_status) values ('morpho',%s,%s,now(),%s)
                   on conflict (product, source_id) do update set last_seen_at=now(), last_status=excluded.last_status, expected_hours=excluded.expected_hours, updated_at=now()""", (source, hours, 'ok' if ok else 'broken')); conn.commit()
def ts(v): return dt.datetime.fromtimestamp(int(v), dt.timezone.utc) if v else None
def chains(): return [c['id'] for c in gql('{ chains { id } }')['chains']]

MARKET_Q = """query($chain:[Int!], $first:Int!, $skip:Int!){ markets(first:$first, skip:$skip, where:{chainId_in:$chain}, orderBy: SupplyAssetsUsd, orderDirection: Desc){
  pageInfo{countTotal} items{ marketId lltv listed chain{id} collateralAsset{symbol address} loanAsset{symbol address decimals}
  state{ supplyAssetsUsd borrowAssetsUsd collateralAssetsUsd liquidityAssetsUsd utilization supplyApy borrowApy netSupplyApy fee timestamp } badDebt{usd} } } }"""
VAULT_Q = """query($chain:[Int!], $first:Int!, $skip:Int!){ vaults(first:$first, skip:$skip, where:{chainId_in:$chain}, orderBy: TotalAssetsUsd, orderDirection: Desc){
  pageInfo{countTotal} items{ address name symbol listed chain{id} asset{symbol address}
  state{ totalAssetsUsd apy netApy netApyExcludingRewards fee sharePriceUsd timestamp curator curators{ id name verified } } } } }"""

VAULT_V2_Q = """query($chain:[Int!], $first:Int!, $skip:Int!){ vaultV2s(first:$first, skip:$skip, where:{chainId_in:$chain}, orderBy: TotalAssetsUsd, orderDirection: Desc){
  pageInfo{countTotal} items{ address name symbol listed type chain{id} asset{symbol address} totalAssetsUsd idleAssetsUsd sharePrice apy netApy netApyExcludingRewards
  performanceFee managementFee curator{address} curators{ items{ id name verified } } } } }"""

def page(query, chain, key):
    skip, out = 0, []
    while True:
        d = gql(query, {'chain': [chain], 'first': 100, 'skip': skip})[key]
        out += d['items']; skip += 100
        if skip >= d['pageInfo']['countTotal'] or not d['items']: break
    return out

failures = []
if ALL or a.markets:
    run = open_run('morpho.markets'); n = 0; fetched = dt.datetime.now(dt.timezone.utc)
    try:
        for ch in chains():
            rows = []
            for m in page(MARKET_Q, ch, 'markets'):
                if not FULL and not m.get('listed'): continue
                s = m.get('state') or {}; ca = m.get('collateralAsset') or {}; la = m.get('loanAsset') or {}
                rows.append((run, fetched, ch, m['marketId'], m.get('listed'), ca.get('symbol'), ca.get('address'), la.get('symbol'), la.get('address'), la.get('decimals'),
                             m.get('lltv'), s.get('supplyAssetsUsd'), s.get('borrowAssetsUsd'), s.get('collateralAssetsUsd'), s.get('liquidityAssetsUsd'), s.get('utilization'),
                             s.get('supplyApy'), s.get('borrowApy'), s.get('netSupplyApy'), s.get('fee'), (m.get('badDebt') or {}).get('usd'), ts(s.get('timestamp')), json.dumps(m) if (FULL and m.get('listed')) else None))
            psycopg2.extras.execute_values(cur, """insert into morpho.raw_market_snapshots (run_id, fetched_at, chain_id, market_id, listed, collateral_symbol, collateral_address,
                loan_symbol, loan_address, loan_decimals, lltv, supply_assets_usd, borrow_assets_usd, collateral_assets_usd, liquidity_assets_usd, utilization,
                supply_apy, borrow_apy, net_supply_apy, fee, bad_debt_usd, state_timestamp, payload) values %s""", rows, page_size=500)
            conn.commit(); n += len(rows); print(f'[markets] chain {ch}: {len(rows)}')
        close_run(run, 'ok', n); freshness('morpho_graphql', True, 3)
    except Exception as e:
        conn.rollback(); close_run(run, 'error', n, str(e)); freshness('morpho_graphql', False, 3); failures.append(f'markets: {e}'); print('[markets] FAILED', e)

if ALL or a.vaults:
    run = open_run('morpho.vaults'); n = 0; fetched = dt.datetime.now(dt.timezone.utc)
    try:
        for ch in chains():
            rows = []
            for v in page(VAULT_Q, ch, 'vaults'):
                if not FULL and not v.get('listed'): continue
                s = v.get('state') or {}; curs = s.get('curators') or []; asset = v.get('asset') or {}
                rows.append((run, fetched, ch, v['address'], v.get('name'), v.get('symbol'), v.get('listed'), asset.get('symbol'), asset.get('address'),
                             s.get('totalAssetsUsd'), s.get('apy'), s.get('netApy'), s.get('netApyExcludingRewards'), s.get('fee'), s.get('sharePriceUsd'), s.get('curator'),
                             [c.get('id') for c in curs], [c.get('name') for c in curs], ts(s.get('timestamp')), json.dumps(v) if (FULL and v.get('listed')) else None))
            psycopg2.extras.execute_values(cur, """insert into morpho.raw_vault_snapshots (run_id, fetched_at, chain_id, vault_address, name, symbol, listed, asset_symbol, asset_address,
                total_assets_usd, apy, net_apy, net_apy_excl_rewards, fee, share_price_usd, curator_address, curator_ids, curator_names, state_timestamp, payload) values %s""", rows, page_size=500)
            conn.commit(); n += len(rows)
            v2rows = []
            for v in page(VAULT_V2_Q, ch, 'vaultV2s'):
                if not FULL and not v.get('listed'): continue
                curs = ((v.get('curators') or {}).get('items')) or []; asset = v.get('asset') or {}
                v2rows.append((run, fetched, ch, v['address'], v.get('name'), v.get('symbol'), v.get('listed'), asset.get('symbol'), asset.get('address'),
                               v.get('totalAssetsUsd'), v.get('apy'), v.get('netApy'), v.get('netApyExcludingRewards'), v.get('performanceFee'), v.get('sharePrice'), (v.get('curator') or {}).get('address'),
                               [c.get('id') for c in curs], [c.get('name') for c in curs], None, json.dumps(v) if (FULL and v.get('listed')) else None, 2, v.get('managementFee'), v.get('idleAssetsUsd')))
            psycopg2.extras.execute_values(cur, """insert into morpho.raw_vault_snapshots (run_id, fetched_at, chain_id, vault_address, name, symbol, listed, asset_symbol, asset_address,
                total_assets_usd, apy, net_apy, net_apy_excl_rewards, fee, share_price_usd, curator_address, curator_ids, curator_names, state_timestamp, payload, vault_version, management_fee, idle_assets_usd) values %s""", v2rows, page_size=500)
            conn.commit(); n += len(v2rows); print(f'[vaults] chain {ch}: v1 {len(rows)}, v2 {len(v2rows)}')
        close_run(run, 'ok', n)
    except Exception as e:
        conn.rollback(); close_run(run, 'error', n, str(e)); failures.append(f'vaults: {e}'); print('[vaults] FAILED', e)

if ALL or a.curators:
    run = open_run('morpho.curators'); n = 0
    try:
        d = gql('{ curators(first: 500){ items{ id name verified addresses{address chainId} state{ aum } } } }')['curators']['items']
        psycopg2.extras.execute_values(cur, """insert into morpho.raw_curators (curator_id, name, verified, addresses, aum, fetched_at, run_id) values %s
            on conflict (curator_id) do update set name=excluded.name, verified=excluded.verified, addresses=excluded.addresses, aum=excluded.aum, fetched_at=excluded.fetched_at, run_id=excluded.run_id""",
            [(c['id'], c.get('name'), c.get('verified'), json.dumps(c.get('addresses')), (c.get('state') or {}).get('aum'), dt.datetime.now(dt.timezone.utc), run) for c in d])
        conn.commit(); n = len(d); close_run(run, 'ok', n); print(f'[curators] {n}')
    except Exception as e:
        conn.rollback(); close_run(run, 'error', n, str(e)); failures.append(f'curators: {e}'); print('[curators] FAILED', e)

# DefiLlama comparators now load once for every product via scripts/ingest_defillama.py into ref.raw_defillama_tvl.

sys.exit(1 if failures else 0)
