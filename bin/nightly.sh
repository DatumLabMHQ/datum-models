#!/usr/bin/env bash
# Nightly hot/cold tiering: raw rows older than tiering.yml hot_days go to R2 as Parquet, verified,
# receipted, then removed from Neon. Dry-run when R2 secrets are absent.
set -uo pipefail
cd "$(dirname "$0")/.."
[ -n "${DATABASE_URL:-}" ] || { echo "DATABASE_URL not set"; exit 2; }
python scripts/tier_cold.py --check || true
if [ -n "${R2_BUCKET:-}" ] && [ -n "${R2_ACCESS_KEY_ID:-}" ]; then python scripts/tier_cold.py; else echo "::warning::R2 secrets not set; dry-run only"; python scripts/tier_cold.py --dry-run; fi
