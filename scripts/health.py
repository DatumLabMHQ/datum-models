"""Health gate for the platform. Exit non-zero when something needs a human.

Checks, against ops.*:
  - any ingest or dbt run in the last window ended in error
  - any source in ops.source_freshness is older than its expected_hours
  - no successful dbt.build in the last 2 hours (when --require-build is set)
Prints a one-line summary per problem so the CI log and the GitHub failure email say what broke.
"""
import argparse, os, sys
import psycopg2
p = argparse.ArgumentParser(); p.add_argument('--window-hours', type=float, default=2); p.add_argument('--require-build', action='store_true')
a = p.parse_args()
url = os.environ.get('DATABASE_URL')
if not url: sys.exit('DATABASE_URL not set')
problems = []
with psycopg2.connect(url) as c, c.cursor() as cur:
    cur.execute("""select job, left(error, 160) from ops.sync_runs
                   where status = 'error' and started_at > now() - make_interval(hours => %s) order by run_id""", (a.window_hours,))
    for job, err in cur.fetchall(): problems.append(f'run error: {job}: {err}')
    cur.execute("""select product, source_id, expected_hours, round(extract(epoch from now() - last_seen_at)/3600, 1), last_status
                   from ops.source_freshness
                   where last_seen_at < now() - make_interval(hours => expected_hours) or last_status = 'broken' order by 1, 2""")
    for prod, src, exp, age, st in cur.fetchall(): problems.append(f'stale source: {prod}/{src} last seen {age}h ago (expected {exp}h, status {st})')
    if a.require_build:
        cur.execute("select 1 from ops.sync_runs where job = 'dbt.build' and status = 'ok' and finished_at > now() - interval '2 hours' limit 1")
        if not cur.fetchone(): problems.append('no successful dbt.build in the last 2 hours')
for line in problems: print('::error::' + line)
print('health:', 'OK' if not problems else f'{len(problems)} problem(s)')
sys.exit(1 if problems else 0)
