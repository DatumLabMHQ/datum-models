"""Write one ops.sync_runs row for a finished job. Used by CI steps that are not Python."""
import argparse, os, sys, json
import psycopg2
p = argparse.ArgumentParser()
p.add_argument('--job', required=True); p.add_argument('--status', required=True)
p.add_argument('--product'); p.add_argument('--runner', default='local'); p.add_argument('--rows', type=int); p.add_argument('--error')
a = p.parse_args()
url = os.environ.get('DATABASE_URL')
if not url: sys.exit('DATABASE_URL not set')
status = {'success': 'ok', 'failure': 'error', 'cancelled': 'skipped'}.get(a.status, a.status)
with psycopg2.connect(url) as c, c.cursor() as cur:
    cur.execute("""insert into ops.sync_runs (job, product, finished_at, status, rows_written, error, runner)
                   values (%s, %s, now(), %s, %s, %s, %s)""", (a.job, a.product, status, a.rows, a.error, a.runner))
print('recorded', a.job, status)
