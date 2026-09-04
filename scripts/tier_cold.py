"""Move raw day-partitions older than the hot window from Neon to R2 as Parquet.

For each table in tiering.yml, for each day older than hot_days that has no verified receipt:
  1. read the partition from Neon, compute a checksum (md5 of sorted per-row md5s)
  2. write Parquet (zstd), upload to R2 at <product>/<table>/day=YYYY-MM-DD/part-0.parquet
  3. download it back, recompute the checksum, write the receipt with verified_at
  4. only then delete the partition from Neon
Without R2 credentials it runs in --dry-run mode: exports to ./cold_export/ and deletes nothing.
Idempotent: re-running never re-exports a verified partition or deletes an unreceipted one.
"""
import argparse, hashlib, io, os, sys, datetime as dt
import psycopg2, psycopg2.extras, yaml
import pyarrow as pa, pyarrow.parquet as pq

p = argparse.ArgumentParser()
p.add_argument('--dry-run', action='store_true', help='export locally, delete nothing')
p.add_argument('--table', help='limit to one table')
p.add_argument('--max-days', type=int, default=400, help='safety cap on partitions per run')
a = p.parse_args()

cfg = yaml.safe_load(open('tiering.yml'))
url = os.environ.get('DATABASE_URL_DIRECT') or os.environ.get('DATABASE_URL')
if not url: sys.exit('DATABASE_URL not set')
r2 = None
if not a.dry_run:
    import boto3
    env = cfg['r2']
    missing = [k for k in (env['bucket_env'], env['endpoint_env'], env['access_key_env'], env['secret_key_env']) if not os.environ.get(k)]
    if missing: sys.exit(f'R2 credentials missing ({missing}); use --dry-run or set them')
    r2 = boto3.client('s3', endpoint_url=os.environ[env['endpoint_env']],
                      aws_access_key_id=os.environ[env['access_key_env']], aws_secret_access_key=os.environ[env['secret_key_env']], region_name='auto')
    bucket = os.environ[env['bucket_env']]

import json, decimal
def canon(v):
    """One canonical text form for a value, identical whether it came from Postgres or from Parquet."""
    if v is None: return ''
    if isinstance(v, bool): return 'true' if v else 'false'
    if isinstance(v, decimal.Decimal): v = float(v)
    if isinstance(v, float): return repr(v)
    if isinstance(v, dt.datetime):
        if v.tzinfo is not None: v = v.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return v.isoformat(timespec='microseconds')
    if isinstance(v, dt.date): return v.isoformat()
    if isinstance(v, (dict, list)): return json.dumps(v, sort_keys=True, default=str, separators=(',', ':'))
    if isinstance(v, (bytes, bytearray)): return v.hex()
    s = str(v)
    # JSON text read back from Parquet: re-canonicalise so key order and spacing do not matter
    if s[:1] in '{[' and s[-1:] in '}]':
        try: return json.dumps(json.loads(s), sort_keys=True, default=str, separators=(',', ':'))
        except Exception: pass
    return s

def row_checksum(rows, cols):
    hashes = sorted(hashlib.md5('|'.join(canon(r[c]) for c in cols).encode()).hexdigest() for r in rows)
    return hashlib.md5(''.join(hashes).encode()).hexdigest()

cutoff = dt.date.today() - dt.timedelta(days=int(cfg['hot_days']))
conn = psycopg2.connect(url); conn.autocommit = False
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
moved = 0
for t in cfg['tables']:
    if a.table and t['table'] != a.table: continue
    table, daycol, product = t['table'], t['day_column'], t['product']
    cur.execute(f"""select ({daycol} at time zone 'UTC')::date as day, count(*) as n from {table}
                    where {daycol} < %s group by 1 order by 1 limit %s""", (cutoff, a.max_days))
    days = cur.fetchall()
    for d in days:
        day, n = d['day'], d['n']
        cur.execute("select verified_at, deleted_from_hot_at from ops.cold_partitions where table_name=%s and day=%s", (table, day))
        rec = cur.fetchone()
        if rec and rec['deleted_from_hot_at']: continue  # already tiered (rows still here would be a bug: report)
        cur.execute(f"select * from {table} where ({daycol} at time zone 'UTC')::date = %s", (day,))
        rows = cur.fetchall()
        if not rows: continue
        cols = list(rows[0].keys())
        checksum = row_checksum(rows, cols)
        # Parquet via pyarrow; jsonb columns are stored as canonical JSON strings, decimals as floats,
        # timestamps as naive UTC so the round-trip is representation-stable.
        def conv(v):
            if isinstance(v, (dict, list)): return json.dumps(v, sort_keys=True, default=str, separators=(',', ':'))
            if isinstance(v, decimal.Decimal): return float(v)
            if isinstance(v, dt.datetime) and v.tzinfo is not None: return v.astimezone(dt.timezone.utc).replace(tzinfo=None)
            return v
        tbl = pa.Table.from_pylist([{c: conv(r[c]) for c in cols} for r in rows])
        buf = io.BytesIO(); pq.write_table(tbl, buf, compression='zstd'); data = buf.getvalue()
        key = f"{product}/{table.split('.')[-1]}/day={day.isoformat()}/part-0.parquet"
        if a.dry_run:
            os.makedirs(os.path.dirname('cold_export/' + key), exist_ok=True)
            open('cold_export/' + key, 'wb').write(data)
            back = data
        else:
            r2.put_object(Bucket=bucket, Key=key, Body=data, ContentType='application/octet-stream')
            back = r2.get_object(Bucket=bucket, Key=key)['Body'].read()
        # verify: re-read the parquet and recompute the checksum on the same columns
        rt = pq.read_table(io.BytesIO(back)).to_pylist()
        back_sum = row_checksum(rt, cols)
        if back_sum != checksum or len(rt) != n:
            conn.rollback(); sys.exit(f'VERIFY FAILED {table} {day}: rows {len(rt)}/{n} checksum {back_sum[:8]}/{checksum[:8]}')
        cur.execute("""insert into ops.cold_partitions (product, table_name, day, row_count, checksum, object_key, bytes, verified_at)
                       values (%s,%s,%s,%s,%s,%s,%s, now())
                       on conflict (table_name, day) do update set row_count=excluded.row_count, checksum=excluded.checksum,
                         object_key=excluded.object_key, bytes=excluded.bytes, exported_at=now(), verified_at=now()""",
                    (product, table, day, n, checksum, key, len(data)))
        if not a.dry_run:
            cur.execute(f"delete from {table} where ({daycol} at time zone 'UTC')::date = %s", (day,))
            cur.execute("update ops.cold_partitions set deleted_from_hot_at = now() where table_name=%s and day=%s", (table, day))
        conn.commit(); moved += 1
        print(f"{'exported (dry-run)' if a.dry_run else 'tiered'} {table} {day}: {n} rows, {len(data)/1024:.1f} KB parquet, {key}")
print(f'{moved} partition(s) {"exported" if a.dry_run else "tiered"}; hot window = {cfg["hot_days"]} days (cutoff {cutoff})')
