"""Turn DATABASE_URL (or DATABASE_URL_DIRECT) into the PG* variables dbt reads.

Usage:  eval "$(python scripts/pgenv.py)"   or   python scripts/pgenv.py --direct
Prints export lines; never prints the password anywhere else.
"""
import os, sys
from urllib.parse import urlparse, parse_qs

key = 'DATABASE_URL_DIRECT' if '--direct' in sys.argv else 'DATABASE_URL'
url = os.environ.get(key) or os.environ.get('DATABASE_URL')
if not url:
    sys.exit(f'{key} is not set')
u = urlparse(url)
print(f'export PGHOST={u.hostname}')
print(f'export PGPORT={u.port or 5432}')
print(f'export PGUSER={u.username}')
print(f"export PGPASSWORD='{u.password}'")
print(f'export PGDATABASE={u.path.lstrip("/")}')
