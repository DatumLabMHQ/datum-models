"""Apply db/migrations/*.sql in filename order, once each.

Tracks applied files in ops.schema_migrations. Uses DATABASE_URL_DIRECT when set (the
non-pooled endpoint, which is what DDL wants), else DATABASE_URL.
"""
import os, sys, glob, hashlib
import psycopg2

CHECK = '--check' in sys.argv
url = os.environ.get('DATABASE_URL_DIRECT') or os.environ.get('DATABASE_URL')
if not url:
    sys.exit('DATABASE_URL_DIRECT or DATABASE_URL must be set')

conn = psycopg2.connect(url)
conn.autocommit = False
cur = conn.cursor()
cur.execute('create schema if not exists ops')
cur.execute("""
  create table if not exists ops.schema_migrations (
    filename text primary key,
    checksum text not null,
    applied_at timestamptz not null default now()
  )""")
conn.commit()

cur.execute('select filename, checksum from ops.schema_migrations')
applied = dict(cur.fetchall())
pending = 0
for path in sorted(glob.glob(os.path.join(os.path.dirname(__file__), '..', 'db', 'migrations', '*.sql'))):
    name = os.path.basename(path)
    sql = open(path).read()
    checksum = hashlib.sha256(sql.encode()).hexdigest()[:16]
    if name in applied:
        if applied[name] != checksum:
            sys.exit(f'{name} was edited after being applied. Add a new migration instead.')
        continue
    if CHECK:
        # Parse and run inside a transaction that is rolled back: proves the SQL is valid against the live schema.
        try:
            cur.execute(sql); conn.rollback(); pending += 1; print('would apply', name)
        except Exception as e:
            conn.rollback(); sys.exit(f'{name} would fail: {e}')
        continue
    try:
        cur.execute(sql)
        cur.execute('insert into ops.schema_migrations (filename, checksum) values (%s, %s)', (name, checksum))
        conn.commit()
        pending += 1
        print('applied', name)
    except Exception as e:
        conn.rollback()
        sys.exit(f'{name} failed: {e}')
print(f"{pending} migration(s) {'pending' if CHECK else 'applied'}, {len(applied)} already in place")
