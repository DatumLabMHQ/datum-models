# Sui ingest

The Sui adapters (one per protocol, knowing every quirk) live in the SuiLending repository.
Until they are extracted into a shared `@datumlabs/sources` package, the platform runs
SuiLending's `scripts/platform-ingest.ts` against a **pinned commit** so a change to the
dashboard repo cannot silently change what the platform ingests.

- `SOURCE_REF` holds the commit SHA (or branch, only while bootstrapping) the hourly run checks out.
- Bump it deliberately in a pull request, with the SuiLending commit message in the PR body.
- The script's contract is documented in `../README.md`: run log, cursors, freshness, append-only raw.

Extraction plan: move `src/protocols/*` and `src/lib/{rpc,sui-client,prices}.ts` from SuiLending
into `packages/sources` here, publish to the org registry, and have both the dashboard and the
ingest job depend on it. Scheduled after the RWA terminal seed.
