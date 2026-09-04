"""Load DefiLlama per-chain TVL (and borrowed) for every slug in defillama_slugs.yml into
ref.raw_defillama_tvl, versioned per run. Skips DefiLlama's aggregate keys (borrowed, staking,
pool2, ...) which are not chains. Last 400 days, excluding today's partial point.

  DATABASE_URL=... python scripts/ingest_defillama.py
"""
import os, sys, json, time, datetime as dt
import requests, psycopg2, psycopg2.extras, yaml

NON_CHAIN_KEYS = ('borrowed', 'staking', 'pool2', 'vesting', 'offers', 'treasury', 'doublecounted', 'liquidstaking')
slugs = yaml.safe_load(open(os.path.join(os.path.dirname(__file__), '..', 'defillama_slugs.yml')))['slugs']
url = os.environ.get('DATABASE_URL_DIRECT') or os.environ.get('DATABASE_URL')
if not url: sys.exit('DATABASE_URL not set')
conn = psycopg2.connect(url); cur = conn.cursor()
RUNNER = 'github-actions' if os.environ.get('GITHUB_ACTIONS') else 'local'
cur.execute("insert into ops.sync_runs (job, product, runner, notes) values ('ref.defillama_tvl','ref',%s,%s) returning run_id", (RUNNER, json.dumps({'slugs': slugs}))); conn.commit(); run = cur.fetchone()[0]
cutoff = int(time.time()) - 400 * 86400; n = 0; failures = []
for slug in slugs:
    try:
        r = requests.get(f'https://api.llama.fi/protocol/{slug}', timeout=60); r.raise_for_status(); j = r.json()
        fetched = dt.datetime.now(dt.timezone.utc); rows = []; ct = j.get('chainTvls') or {}
        cur.execute("""select distinct on (chain, day) chain, day, tvl_usd, borrowed_usd from ref.raw_defillama_tvl where slug=%s order by chain, day, fetched_at desc""", (slug,))
        latest = {(c, d): (t, b) for c, d, t, b in cur.fetchall()}
        for chain, series in ct.items():
            if chain.lower() in NON_CHAIN_KEYS or any(chain.endswith('-' + s) for s in NON_CHAIN_KEYS): continue
            borrowed = {p['date']: p['totalLiquidityUSD'] for p in (ct.get(chain + '-borrowed') or {}).get('tvl', [])}
            for p in [p for p in (series.get('tvl') or []) if p['date'] >= cutoff][:-1]:
                day = dt.datetime.fromtimestamp(p['date'], dt.timezone.utc).date(); val = (p['totalLiquidityUSD'], borrowed.get(p['date']))
                if latest.get((chain.lower(), day)) == val: continue   # unchanged since the last stored version
                rows.append((slug, chain.lower(), day, val[0], val[1], fetched, run))
        psycopg2.extras.execute_values(cur, "insert into ref.raw_defillama_tvl (slug, chain, day, tvl_usd, borrowed_usd, fetched_at, run_id) values %s on conflict do nothing", rows, page_size=2000)
        conn.commit(); n += len(rows); print(f'[defillama] {slug}: {len(rows)} new or changed chain-days')
    except Exception as e:
        conn.rollback(); failures.append(f'{slug}: {e}'); print(f'[defillama] {slug} FAILED', e)
ok = not failures
cur.execute("update ops.sync_runs set finished_at=now(), status=%s, rows_written=%s, error=%s where run_id=%s", ('ok' if ok else 'error', n, '; '.join(failures) or None, run))
cur.execute("""insert into ops.source_freshness (product, source_id, expected_hours, last_seen_at, last_status) values ('ref','defillama_protocol',36,now(),%s)
               on conflict (product, source_id) do update set last_seen_at=now(), last_status=excluded.last_status, updated_at=now()""", ('ok' if ok else 'broken',)); conn.commit()
sys.exit(1 if failures else 0)
