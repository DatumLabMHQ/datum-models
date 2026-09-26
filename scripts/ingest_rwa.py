"""Hourly RWA ingest that reads sources directly, beside the legacy import (scripts/import_legacy_rwa.py):

  rwa.horizon_emode  Aave Horizon e-mode categories from the pool contract (every category, its LTV, liquidation
                     threshold and bonus, and which reserves are collateral or borrowable in it).
  rwa.token_supply   totalSupply of every tokenized asset on every chain in seeds/rwa_token_registry.csv.
  rwa.euler          Euler v2 RWA vaults (seeds/rwa_euler_vaults.csv plus any vault whose asset is a known RWA
                     token) with their accepted collaterals, LTVs and the debt each collateral backs (Euler v3 API).

  DATABASE_URL=... python scripts/ingest_rwa.py [--emode] [--supply] [--euler]

Public RPC endpoints by default; RPC_URL_<chainId> (e.g. RPC_URL_1) overrides one chain.
"""
import argparse, csv, json, os, re, sys, time, datetime as dt
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import requests, psycopg2, psycopg2.extras

ROOT = Path(__file__).resolve().parent.parent
RPC = {1: ['https://ethereum-rpc.publicnode.com', 'https://eth.drpc.org', 'https://eth.llamarpc.com'],
       56: ['https://bsc-rpc.publicnode.com', 'https://bsc.drpc.org'],
       137: ['https://polygon-bor-rpc.publicnode.com', 'https://polygon.drpc.org'],
       43114: ['https://avalanche-c-chain-rpc.publicnode.com', 'https://avalanche.drpc.org'],
       57073: ['https://rpc-gel.inkonchain.com', 'https://ink.drpc.org'],
       'solana': ['https://api.mainnet-beta.solana.com']}
HORIZON = {'chain_id': 1, 'pool': '0xae05cd22df81871bc7cc2a04becfb516bfe332c8'}
EULER = 'https://v3.euler.finance/v3'

p = argparse.ArgumentParser(); p.add_argument('--emode', action='store_true'); p.add_argument('--supply', action='store_true'); p.add_argument('--euler', action='store_true'); a = p.parse_args()
ALL = not (a.emode or a.supply or a.euler)
url = os.environ.get('DATABASE_URL_DIRECT') or os.environ.get('DATABASE_URL')
if not url: sys.exit('DATABASE_URL not set')
conn = psycopg2.connect(url); cur = conn.cursor()
RUNNER = 'github-actions' if os.environ.get('GITHUB_ACTIONS') else 'local'

def open_run(job): cur.execute("insert into ops.sync_runs (job, product, runner, notes) values (%s,'rwa',%s,'{}') returning run_id", (job, RUNNER)); conn.commit(); return cur.fetchone()[0]
def close_run(run_id, status, rows, error=None): cur.execute("update ops.sync_runs set finished_at=now(), status=%s, rows_written=%s, error=%s where run_id=%s", (status, rows, error, run_id)); conn.commit()
def freshness(source, ok, expected=3):
    cur.execute("""insert into ops.source_freshness (product, source_id, expected_hours, last_seen_at, last_status) values ('rwa',%s,%s,now(),%s)
                   on conflict (product, source_id) do update set last_seen_at=now(), last_status=excluded.last_status, updated_at=now()""", (source, expected, 'ok' if ok else 'broken')); conn.commit()
def seed(name): return list(csv.DictReader(open(ROOT / 'seeds' / f'{name}.csv')))

# Function selectors (first 4 bytes of keccak256 of the signature), fixed so the job needs no keccak dependency.
SELECTORS = {'getReservesList()': '0xd1946dbc', 'symbol()': '0x95d89b41', 'totalSupply()': '0x18160ddd',
             'getEModeCategoryCollateralConfig(uint8)': '0xb286f467', 'getEModeCategoryLabel(uint8)': '0x2083e183',
             'getEModeCategoryCollateralBitmap(uint8)': '0xb0771dba', 'totalAssets()': '0x01e1d114', 'getEModeCategoryBorrowableBitmap(uint8)': '0x903a2c71'}
def selector(sig): return SELECTORS[sig]
def rpc(chain, method, params):
    urls = ([os.environ[f'RPC_URL_{chain}']] if os.environ.get(f'RPC_URL_{chain}') else []) + RPC[chain]; last = None
    for u in urls:
        for i in range(2):
            try:
                r = requests.post(u, json={'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params}, timeout=30).json()
                if 'result' in r: return r['result']
                last = r.get('error')
            except Exception as e: last = e
            time.sleep(1 + i)
    raise RuntimeError(f'{chain} {method} failed on every endpoint: {last}')
def call(chain, to, sig, arg_hex=''): return rpc(chain, 'eth_call', [{'to': to, 'data': selector(sig) + arg_hex}, 'latest'])
def word(h, i): return int(h[2 + 64 * i: 2 + 64 * (i + 1)], 16)
def string(h):
    b = bytes.fromhex(h[2:]); n = int.from_bytes(b[32:64], 'big'); return b[64:64 + n].decode(errors='replace')
def uint8(i): return hex(i)[2:].rjust(64, '0')

failures = []

if ALL or a.emode:
    run = open_run('rwa.horizon_emode'); n = 0; fetched = dt.datetime.now(dt.timezone.utc)
    try:
        c, pool = HORIZON['chain_id'], HORIZON['pool']
        h = call(c, pool, 'getReservesList()'); count = word(h, 1)
        reserves = ['0x' + h[2 + 64 * (2 + i) + 24: 2 + 64 * (3 + i)] for i in range(count)]
        symbols = [string(call(c, r, 'symbol()')) for r in reserves]
        rows = []
        # Category 0 is "no e-mode". Categories are dense from 1; an unconfigured id reads back all zeros with no label.
        for cid in range(1, 256):
            cfg = call(c, pool, 'getEModeCategoryCollateralConfig(uint8)', uint8(cid))
            ltv, lt, bonus = word(cfg, 0), word(cfg, 1), word(cfg, 2)
            label = string(call(c, pool, 'getEModeCategoryLabel(uint8)', uint8(cid)))
            if not ltv and not lt and not label: break
            coll = int(call(c, pool, 'getEModeCategoryCollateralBitmap(uint8)', uint8(cid)), 16)
            borr = int(call(c, pool, 'getEModeCategoryBorrowableBitmap(uint8)', uint8(cid)), 16)
            for i, (r, s) in enumerate(zip(reserves, symbols)):
                for role, bits in (('collateral', coll), ('borrowable', borr)):
                    if bits >> i & 1:
                        rows.append((run, fetched, c, pool, cid, label, ltv / 100, lt / 100, (bonus - 10000) / 100 if bonus else None, r, s, role))
        psycopg2.extras.execute_values(cur, """insert into rwa.raw_horizon_emode (run_id, fetched_at, chain_id, pool, category_id, label, ltv_pct,
            liquidation_threshold_pct, liquidation_bonus_pct, reserve, symbol, role) values %s""", rows)
        conn.commit(); n = len(rows); close_run(run, 'ok', n); freshness('horizon_rpc', True)
        print(f'[rwa emode] {len({r[4] for r in rows})} categories, {n} category x reserve rows')
    except Exception as e:
        conn.rollback(); close_run(run, 'error', n, str(e)); freshness('horizon_rpc', False); failures.append(f'emode: {e}'); print('[rwa emode] FAILED', e)

if ALL or a.supply:
    run = open_run('rwa.token_supply'); n = 0; fetched = dt.datetime.now(dt.timezone.utc); errors = []
    try:
        rows = []
        for t in seed('rwa_token_registry'):
            try:
                if t['chain'] == 'solana':
                    v = rpc('solana', 'getTokenSupply', [t['address']])['value']; supply = v['uiAmountString']
                else:
                    chain = int(t['chain']); raw = int(call(chain, t['address'], 'totalSupply()'), 16); supply = str(raw / 10 ** int(t['decimals']))
                rows.append((run, fetched, t['ticker'], t['chain'], t['address'].lower() if t['chain'] != 'solana' else t['address'], supply))
            except Exception as e: errors.append(f"{t['ticker']}@{t['chain']}: {e}")
        # A missing chain would understate an asset's AUM, so a run writes only when every read succeeded; the mart
        # carries the last complete run forward with its timestamp.
        if errors: raise RuntimeError('; '.join(errors))
        psycopg2.extras.execute_values(cur, "insert into rwa.raw_token_supply (run_id, fetched_at, ticker, chain, address, total_supply) values %s", rows)
        conn.commit(); n = len(rows); close_run(run, 'ok', n); freshness('chain_rpc', True)
        print(f'[rwa supply] {n} token x chain reads')
    except Exception as e:
        conn.rollback(); close_run(run, 'error', n, str(e)); freshness('chain_rpc', False); failures.append(f'supply: {e}'); print('[rwa supply] FAILED', e)

if ALL or a.euler:
    # Which Euler vaults are RWA is data, not code: a vault is RWA when its asset (chain, address) is in
    # seeds/rwa_euler_assets.csv. A cluster is an active (non-deprecated) Euler label product that holds an RWA vault,
    # or whose vaults lend against RWA collateral. Vaults whose asset merely looks like an RWA by name are written as
    # role 'candidate' so a person can add them to the seed; they are never counted.
    run = open_run('rwa.euler'); n = 0; fetched = dt.datetime.now(dt.timezone.utc)
    CHAINS = [1, 42161, 8453, 137, 143, 999, 56, 130, 9745, 43114, 59144]
    CANDIDATE = ('(Ondo Tokenized)', 'xStock', '(Reality Tokenized)', 'ST0x', 'deRWA', 'Centrifuge', 'Securitize', 'Tokenized', 'Treasury', 'T-Bill')
    def euler(path, params=None, tries=4):
        last = None
        for i in range(tries):
            try:
                r = requests.get(f'{EULER}/{path}', params=params, timeout=60)
                if r.status_code == 200: return r.json()
                last = f'{r.status_code} {r.text[:120]}'
            except Exception as e: last = e
            time.sleep(1.5 * (i + 1))
        raise RuntimeError(f'euler {path} failed: {last}')
    def pages(path, params=None):
        out, off = [], 0
        while True:
            d = euler(path, {**(params or {}), 'limit': 100, 'offset': off}); out += d['data']; off += 100
            if off >= d['meta'].get('total', 0): return out
    try:
        rwa_assets = {(int(r['chain_id']), r['asset_address'].lower()): r for r in seed('rwa_euler_assets')}
        products = {(p['chainId'], p['id']): p for p in pages('labels/products')}
        labels = {(v['chainId'], v['address'].lower()): v for v in pages('labels/vaults')}
        entities = {e['id']: e['name'] for e in pages('labels/entities')}
        vaults = {}
        for c in CHAINS:
            for v in pages('evk/vaults', {'chainId': c, 'visibility': 'visible,warning,hidden,pending_review'}): vaults[(c, v['address'].lower())] = v
        # Securitize collateral vaults are not in /evk/vaults; their token balance sits in the vault (ERC-4626 totalAssets).
        sec = [a for (c, a), l in labels.items() if c == 1 and l.get('vaultType') == 'securitize']
        if sec:
            r = requests.post(f'{EULER}/securitize/vaults/batch', json={'chainId': 1, 'addresses': sec}, timeout=60).json()
            for v in r.get('data') or []:
                ta = int(call(1, v['address'], 'totalAssets()'), 16) if v.get('address') else None
                vaults[(1, v['address'].lower())] = {**v, 'totalAssets': str(ta) if ta is not None else None, 'totalSupplyUsd': 0.0 if ta == 0 else None, 'totalBorrowsUsd': 0.0}
        def product(k):
            l = labels.get(k) or {}; p = products.get((k[0], l.get('productId'))) or {}
            active = bool(p) and not p.get('isDeprecated') and not l.get('isDeprecated')
            ent = p.get('entityId') or l.get('entityId'); ent = ent[0] if isinstance(ent, list) else ent
            return (p.get('name') if active else None), entities.get(ent, ent)
        asset = lambda v: (v.get('asset') or {}) if isinstance(v.get('asset'), dict) else {'address': v.get('asset')}
        is_rwa = lambda k: (k[0], (asset(vaults[k]).get('address') or '').lower()) in rwa_assets
        # Open interest per chain: borrow vault -> collateral vault -> USD debt that collateral backs.
        oi = {}
        for c in CHAINS:
            for b, cols in (euler('evk/vaults/open-interest/by-collateral', {'chainId': c}).get('data') or {}).items():
                for col, usd in cols.items(): oi[(c, b.lower(), col.lower())] = usd
        rwa_keys = {k for k in vaults if is_rwa(k) and product(k)[0]}
        clusters = {(k[0], (labels.get(k) or {}).get('productId')) for k in rwa_keys}
        # Borrow vaults that carry debt against an RWA collateral join too (e.g. KPK lending against Securitize vaults).
        for (c, b, col) in oi:
            if (c, col) in rwa_keys and (c, b) in vaults and product((c, b))[0]: clusters.add((c, (labels.get((c, b)) or {}).get('productId')))
        # Named RWA products with no RWA vault of their own and no debt yet (KPK x Securitize lends against the Securitize vaults).
        for (c, pid), pr in products.items():
            if not pr.get('isDeprecated') and re.search(r'RWA|Securitize|Tenbin|Hybond', pr.get('name') or '', re.I): clusters.add((c, pid))
        members = {k for k, l in labels.items() if (k[0], l.get('productId')) in clusters and k in vaults and product(k)[0]}
        vrows, crows = [], []
        for k in sorted(members | rwa_keys):
            v = vaults[k]; a_ = asset(v); name, curator = product(k)
            vrows.append((run, fetched, k[0], k[1], (a_.get('address') or '').lower(), a_.get('symbol'), a_.get('decimals'), v.get('totalAssets'), v.get('totalBorrows'),
                          v.get('totalSupplyUsd'), v.get('totalBorrowsUsd'), v.get('utilization'), v.get('supplyApy'), v.get('borrowApy'),
                          name, curator, 'collateral' if k in rwa_keys else 'borrowable', False, json.dumps(v)))
        def collaterals(k):
            for i in range(3):   # the endpoint sometimes answers an empty list under load; retry before believing it
                cols = euler(f'evk/vaults/{k[0]}/{k[1]}/collaterals').get('data') or []
                if cols or i == 2: return k, cols
                time.sleep(0.5)
        evk = [k for k in sorted(members | rwa_keys) if vaults[k].get('vaultType', 'evk') == 'evk']
        with ThreadPoolExecutor(max_workers=6) as pool:   # six at a time answers completely; sixteen did not
            for k, cols in pool.map(collaterals, evk):
                for col in cols:
                    bl, ll = int(col.get('borrowLTV') or 0) / 1e4, int(col.get('liquidationLTV') or 0) / 1e4
                    if not bl and not ll: continue   # a collateral that was switched off
                    ck = col['collateral'].lower()
                    crows.append((run, fetched, k[0], k[1], ck, bl, ll, oi.get((k[0], k[1], ck))))
        for k, v in vaults.items():   # candidates: RWA-looking assets outside the seed, with money in them
            a_ = asset(v)
            if is_rwa(k) or (v.get('totalSupplyUsd') or 0) < 1000 or not any(t in (a_.get('name') or '') for t in CANDIDATE): continue
            name, curator = product(k)
            vrows.append((run, fetched, k[0], k[1], (a_.get('address') or '').lower(), a_.get('symbol'), a_.get('decimals'), v.get('totalAssets'), v.get('totalBorrows'),
                          v.get('totalSupplyUsd'), v.get('totalBorrowsUsd'), v.get('utilization'), v.get('supplyApy'), v.get('borrowApy'), name, curator, 'candidate', True, json.dumps(v)))
        psycopg2.extras.execute_values(cur, """insert into rwa.raw_euler_vaults (run_id, fetched_at, chain_id, vault, asset_address, asset_symbol, asset_decimals, total_assets,
            total_borrows, supply_usd, borrow_usd, utilization, supply_apy, borrow_apy, cluster, curator, role, discovered, payload) values %s""", vrows)
        psycopg2.extras.execute_values(cur, """insert into rwa.raw_euler_collaterals (run_id, fetched_at, chain_id, borrow_vault, collateral_vault, borrow_ltv, liquidation_ltv,
            debt_backed_usd) values %s""", crows)
        conn.commit(); n = len(vrows) + len(crows); close_run(run, 'ok', n); freshness('euler_v3_api', True)
        print(f'[rwa euler] {len(clusters)} clusters, {len(rwa_keys)} RWA vaults, {len(vrows)} vault rows, {len(crows)} collateral pairs, '
              f"${sum((vaults[k].get('totalSupplyUsd') or 0) for k in rwa_keys)/1e6:.1f}M RWA supplied")
    except Exception as e:
        conn.rollback(); close_run(run, 'error', n, str(e)); freshness('euler_v3_api', False); failures.append(f'euler: {e}'); print('[rwa euler] FAILED', e)

sys.exit(1 if failures else 0)
