-- Shared reference layer. DefiLlama protocol TVL for every product's comparators lives here once,
-- versioned per run; product staging models pick the latest fetch per (slug, chain, day).
create schema if not exists ref;
create table if not exists ref.raw_defillama_tvl (
  slug text not null, chain text not null, day date not null, tvl_usd double precision, borrowed_usd double precision,
  fetched_at timestamptz not null, run_id bigint not null,
  primary key (slug, chain, day, run_id)
);
create index if not exists ref_defillama_latest on ref.raw_defillama_tvl (slug, chain, day, fetched_at desc);
grant usage on schema ref to datum_reader;
alter default privileges in schema ref grant select on tables to datum_reader;
grant select on all tables in schema ref to datum_reader;
