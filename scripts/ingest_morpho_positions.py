"""Morpho positions, twice a day: for the largest listed markets, the largest suppliers and borrowers
with their health factors, from the Morpho GraphQL API. The hourly job calls this every hour; it does
its work at 00:xx and 12:xx UTC and exits at once otherwise, so a failed noon run is caught at midnight.
Append-only raw rows with run logs, like ingest_morpho.py. --dry-run fetches and prints without a database.

  DATABASE_URL=... python scripts/ingest_morpho_positions.py [--full] [--markets 100] [--suppliers 20] [--borrowers 40] [--dry-run]
"""
import argparse, os, sys, json, time, datetime as dt
import requests

API = 'https://blue-api.morpho.org/graphql'
p = argparse.ArgumentParser()
p.add_argument('--full', action='store_true', help='run now, whatever the hour')
p.add_argument('--dry-run', action='store_true', help='fetch and print, write nothing')
p.add_argument('--markets', type=int, default=100, help='largest listed markets by supplied value, all chains')
p.add_argument('--suppliers', type=int, default=20, help='largest suppliers per market, by supply shares')
p.add_argument('--borrowers', type=int, default=40, help='largest borrowers per market, by borrow shares')
a = p.parse_args()
hour = dt.datetime.now(dt.timezone.utc).hour
if not (a.full or a.dry_run) and hour not in (0, 12):
    print(f'[positions] {hour:02d}:xx UTC is not a positions hour (00 or 12); nothing to do'); sys.exit(0)

conn = cur = None
if not a.dry_run:
    import psycopg2, psycopg2.extras
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
    if a.dry_run: return 0
    cur.execute("insert into ops.sync_runs (job, product, runner, notes) values (%s,'morpho',%s,%s) returning run_id", (job, RUNNER, json.dumps(notes or {}))); conn.commit(); return cur.fetchone()[0]
def close_run(run_id, status, rows, error=None):
    if a.dry_run: print(f'[positions] dry run: {status}, {rows} rows, {error or "no error"}'); return
    cur.execute("update ops.sync_runs set finished_at=now(), status=%s, rows_written=%s, error=%s where run_id=%s", (status, rows, error, run_id)); conn.commit()
def freshness(source, ok, hours):
    if a.dry_run: return
    cur.execute("""insert into ops.source_freshness (product, source_id, expected_hours, last_seen_at, last_status) values ('morpho',%s,%s,now(),%s)
                   on conflict (product, source_id) do update set last_seen_at=now(), last_status=excluded.last_status, expected_hours=excluded.expected_hours, updated_at=now()""", (source, hours, 'ok' if ok else 'broken')); conn.commit()
def ts(v): return dt.datetime.fromtimestamp(int(v), dt.timezone.utc) if v else None

MARKETS_Q = """query($first:Int!){ markets(first:$first, where:{listed:true}, orderBy: SupplyAssetsUsd, orderDirection: Desc){
  items{ marketId chain{id} state{ supplyAssetsUsd borrowAssetsUsd } } } }"""
# One query for both sides: the order decides which side we are sampling. Positions with zero shares
# on that side (a market with fewer holders than asked for) are dropped below.
POSITIONS_Q = """query($k:[String!], $c:[Int!], $first:Int!, $ob:MarketPositionOrderBy!){
  marketPositions(first:$first, skip:0, orderBy:$ob, orderDirection: Desc, where:{marketUniqueKey_in:$k, chainId_in:$c}){
    items{ user{address} healthFactor priceVariationToLiquidationPrice
           state{ supplyAssetsUsd borrowAssetsUsd collateralUsd supplyShares borrowShares timestamp } } } }"""

def positions(chain, market, side, first):
    ob = 'SupplyShares' if side == 'supply' else 'BorrowShares'
    items = gql(POSITIONS_Q, {'k': [market], 'c': [chain], 'first': first, 'ob': ob})['marketPositions']['items']
    out = []
    for it in items:
        s = it.get('state') or {}
        shares = s.get('supplyShares') if side == 'supply' else s.get('borrowShares')
        if not shares or float(shares) <= 0: continue
        out.append((it, s))
    return out

run = open_run('morpho.positions', {'markets': a.markets, 'suppliers': a.suppliers, 'borrowers': a.borrowers})
n = 0; fetched = dt.datetime.now(dt.timezone.utc); errors = []
try:
    markets = gql(MARKETS_Q, {'first': a.markets})['markets']['items']
    print(f'[positions] {len(markets)} listed markets, {a.suppliers} suppliers + {a.borrowers} borrowers each')
    for m in markets:
        ch = m['chain']['id']; mid = m['marketId']; st = m.get('state') or {}
        rows = []
        try:
            for side, first in (('supply', a.suppliers), ('borrow', a.borrowers)):
                for rank, (it, s) in enumerate(positions(ch, mid, side, first), start=1):
                    rows.append((run, fetched, ch, mid, it['user']['address'], side, rank,
                                 s.get('supplyAssetsUsd'), s.get('borrowAssetsUsd'), s.get('collateralUsd'), s.get('supplyShares'), s.get('borrowShares'),
                                 it.get('healthFactor'), it.get('priceVariationToLiquidationPrice'), st.get('supplyAssetsUsd'), st.get('borrowAssetsUsd'), ts(s.get('timestamp'))))
        except Exception as e:
            errors.append(f'{ch}-{mid[:10]}: {e}'); print(f'[positions] chain {ch} market {mid[:10]} FAILED {e}'); continue
        if a.dry_run:
            sup = [r for r in rows if r[5] == 'supply']; bor = [r for r in rows if r[5] == 'borrow']
            cov = (sum((r[8] or 0) for r in bor) / st['borrowAssetsUsd'] * 100) if st.get('borrowAssetsUsd') else 0
            hfs = [r[12] for r in bor if r[12] is not None]
            print(f'  chain {ch} {mid[:10]}: {len(sup)} suppliers, {len(bor)} borrowers covering {cov:.0f}% of borrow, min HF {min(hfs) if hfs else None}')
        else:
            psycopg2.extras.execute_values(cur, """insert into morpho.raw_position_snapshots (run_id, fetched_at, chain_id, market_id, user_address, side, rank,
                supply_assets_usd, borrow_assets_usd, collateral_usd, supply_shares, borrow_shares, health_factor, price_to_liquidation,
                market_supply_usd, market_borrow_usd, state_timestamp) values %s""", rows, page_size=500)
            conn.commit()
        n += len(rows)
    close_run(run, 'error' if errors else 'ok', n, '; '.join(errors)[:2000] if errors else None); freshness('morpho_positions', not errors, 14)
    print(f'[positions] {n} rows, {len(errors)} failed markets')
except Exception as e:
    if conn: conn.rollback()
    close_run(run, 'error', n, str(e)); freshness('morpho_positions', False, 14); print('[positions] FAILED', e); sys.exit(1)
sys.exit(1 if errors else 0)
