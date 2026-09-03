"""Drop the ci_<run>_* schemas a pull-request build created."""
import os, sys, psycopg2
run = sys.argv[1] if len(sys.argv) > 1 else None
if not run: sys.exit('run id required')
with psycopg2.connect(os.environ['DATABASE_URL']) as c, c.cursor() as cur:
    cur.execute("select nspname from pg_namespace where nspname like %s", (f'ci_{run}_%',))
    for (name,) in cur.fetchall():
        cur.execute(f'drop schema "{name}" cascade'); print('dropped', name)
