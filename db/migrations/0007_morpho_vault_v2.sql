-- Morpho Vault V2 (3,168 vaults, ~$3.8B listed TVL as of Sept 2026) lives in a separate API type
-- (vaultV2s) with fees split into performance and management. Same snapshot table, versioned.
alter table morpho.raw_vault_snapshots
  add column if not exists vault_version smallint not null default 1,
  add column if not exists management_fee double precision,
  add column if not exists idle_assets_usd double precision;
