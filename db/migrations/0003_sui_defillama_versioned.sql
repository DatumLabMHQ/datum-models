-- Raw must be append-only. raw_defillama_tvl used to upsert in place; it now keeps one row per
-- (protocol, day, run_id) and staging selects the latest fetch. Existing rows are kept.
alter table sui.raw_defillama_tvl drop constraint if exists raw_defillama_tvl_pkey;
alter table sui.raw_defillama_tvl alter column run_id set not null;
alter table sui.raw_defillama_tvl add primary key (protocol, day, run_id);
create index if not exists raw_defillama_tvl_latest on sui.raw_defillama_tvl (protocol, day, fetched_at desc);
