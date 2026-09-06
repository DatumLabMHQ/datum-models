#!/usr/bin/env bash
# The hourly pipeline, end to end, as one script. GitHub Actions and the container both run this,
# so "works on the runner" and "works on my machine" are the same statement.
#
# Required env: DATABASE_URL. Optional: LEGACY_RWA_DATABASE_URL, SUI_GRPC_URL, SUI_GRPC_BEARER,
# CENTRIFUGE_API_URL, CENTRIFUGE_PROXY_KEY, HEALTH_WARN_ONLY, RUNNER (label for ops.sync_runs).
# Flags: --no-sui (skip the SuiLending checkout), --check (print the plan and exit).
set -uo pipefail
cd "$(dirname "$0")/.."
export DBT_TARGET="${DBT_TARGET:-prod}"
RUNNER_LABEL="${RUNNER:-$([ -n "${GITHUB_ACTIONS:-}" ] && echo github-actions || echo container)}"
SKIP_SUI=0; CHECK=0
for a in "$@"; do case "$a" in --no-sui) SKIP_SUI=1;; --check) CHECK=1;; esac; done

step() { echo; echo "── $1"; }
warn() { echo "::warning::$1"; }

if [ "$CHECK" = 1 ]; then
  echo "plan: migrate → sui ingest($([ $SKIP_SUI = 1 ] && echo skipped || echo "SuiLending@$(cat ingest/sui/SOURCE_REF)")) → rwa legacy sync → defillama → morpho → aave → centrifuge → dbt build → freshness → record → health"
  python -c "import dbt.version, psycopg2, pyarrow, boto3, requests, yaml; print('python deps ok; dbt', dbt.version.__version__)"
  node --version; exit 0
fi
[ -n "${DATABASE_URL:-}" ] || { echo "DATABASE_URL not set"; exit 2; }

step "migrate";        python scripts/migrate.py || { echo "migration failed; stopping before any ingest"; exit 1; }

if [ "$SKIP_SUI" = 0 ]; then
  step "sui ingest (SuiLending @ $(cat ingest/sui/SOURCE_REF))"
  if [ ! -d _sui/.git ]; then git clone -q https://github.com/DatumLabMHQ/SuiLending _sui; fi
  ( cd _sui && git fetch -q origin && git checkout -q "$(cat ../ingest/sui/SOURCE_REF)" \
    && npm ci --no-audit --no-fund --silent \
    && PLATFORM_DATABASE_URL="$DATABASE_URL" npx tsx scripts/platform-ingest.ts ) || warn "one or more Sui ingest jobs failed; the health gate will fail the run"
fi

step "rwa legacy sync"; python scripts/import_legacy_rwa.py   || warn "RWA legacy sync had failures (see ops.legacy_imports)"
step "defillama (ref)"; python scripts/ingest_defillama.py    || warn "DefiLlama load had failures (see ops.sync_runs)"
step "morpho";          python scripts/ingest_morpho.py       || warn "Morpho ingest had failures (see ops.sync_runs)"
step "aave";            python scripts/ingest_aave.py         || warn "Aave ingest had failures (see ops.sync_runs)"
step "centrifuge";      python scripts/ingest_centrifuge.py   || warn "Centrifuge ingest had failures (see ops.sync_runs)"

step "dbt build"
eval "$(python scripts/pgenv.py)"
dbt deps --profiles-dir . >/dev/null
BUILD=ok
dbt build --profiles-dir . --target "$DBT_TARGET" || BUILD=failure
step "source freshness"; dbt source freshness --profiles-dir . --target "$DBT_TARGET" || true
step "record run";      python scripts/record_run.py --job dbt.build --status "$([ "$BUILD" = ok ] && echo success || echo failure)" --runner "$RUNNER_LABEL"
step "docs";            dbt docs generate --profiles-dir . --target "$DBT_TARGET" --static >/dev/null || true
step "health gate";     python scripts/health.py --window-hours 2; H=$?
[ "$BUILD" = ok ] || { echo "dbt build failed"; exit 1; }
exit $H
