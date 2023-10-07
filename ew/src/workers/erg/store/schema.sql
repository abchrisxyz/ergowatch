create schema erg;
comment on schema erg is 'ERG balances, age and supply metrics';

-------------------------------------------------------------------------------
-- Revision
-------------------------------------------------------------------------------
create table erg._rev (
	singleton int primary key default 1,
	rev_major integer not null,
	rev_minor integer not null,
	check(singleton = 1)
);
insert into erg._rev (rev_major, rev_minor) values (1, 0);
comment on table erg._rev is 'Current schema revision';


-------------------------------------------------------------------------------
-- Headers
-------------------------------------------------------------------------------
create table erg.headers (
    height integer primary key,
    timestamp bigint not null,
    id text not null
);

-------------------------------------------------------------------------------
-- Balances
-------------------------------------------------------------------------------
create table erg.balances (
    address_id bigint primary key not null,
    -- Balance in nanoERG
    nano bigint not null,
    -- Tracks the mean time of origin of supply in address
    mean_age_timestamp bigint not null,
    -- Balance cannot be negative and we don't keep spent addresses
    check (nano > 0)
);

-------------------------------------------------------------------------------
-- Diffs
-------------------------------------------------------------------------------
create table erg.balance_diffs (
    address_id bigint not null,
    height integer not null,
    -- Index of block transaction
    tx_idx smallint not null,
    -- Balance difference in nanoERG
    nano bigint not null,
    primary key (address_id, height, tx_idx)
);
create index on erg.balance_diffs using brin(height);

-------------------------------------------------------------------------------
-- Address counts
-------------------------------------------------------------------------------
create table erg.address_counts_by_balance_p2pk (
	height integer primary key,
	total bigint not null,
	ge_0p001 bigint not null,
	ge_0p01 bigint not null,
	ge_0p1 bigint not null,
	ge_1 bigint not null,
	ge_10 bigint not null,
	ge_100 bigint not null,
	ge_1k bigint not null,
	ge_10k bigint not null,
	ge_100k bigint not null,
	ge_1m bigint not null
);
create table erg.address_counts_by_balance_contracts (
	height integer primary key,
	total bigint not null,
	ge_0p001 bigint not null,
	ge_0p01 bigint not null,
	ge_0p1 bigint not null,
	ge_1 bigint not null,
	ge_10 bigint not null,
	ge_100 bigint not null,
	ge_1k bigint not null,
	ge_10k bigint not null,
	ge_100k bigint not null,
	ge_1m bigint not null
);
create table erg.address_counts_by_balance_miners (
	height integer primary key,
	total bigint not null,
	ge_0p001 bigint not null,
	ge_0p01 bigint not null,
	ge_0p1 bigint not null,
	ge_1 bigint not null,
	ge_10 bigint not null,
	ge_100 bigint not null,
	ge_1k bigint not null,
	ge_10k bigint not null,
	ge_100k bigint not null,
	ge_1m bigint not null
);

create table erg.address_counts_by_balance_p2pk_summary (
	label text primary key,
	current bigint not null,
	diff_1d bigint not null,
	diff_1w bigint not null,
	diff_4w bigint not null,
	diff_6m bigint not null,
	diff_1y bigint not null
);
create table erg.address_counts_by_balance_contracts_summary (
	label text primary key,
	current bigint not null,
	diff_1d bigint not null,
	diff_1w bigint not null,
	diff_4w bigint not null,
	diff_6m bigint not null,
	diff_1y bigint not null
);
create table erg.address_counts_by_balance_miners_summary (
	label text primary key,
	current bigint not null,
	diff_1d bigint not null,
	diff_1w bigint not null,
	diff_4w bigint not null,
	diff_6m bigint not null,
	diff_1y bigint not null
);


-------------------------------------------------------------------------------
-- Supply composition
-------------------------------------------------------------------------------
create table erg.supply_composition (
	height integer primary key,
	-- All p2pk's, including cex's
	p2pks bigint not null,
	-- Non-mining contracts, excluding (re)-emission
	contracts bigint not null,
	-- Mining contracts
	miners bigint not null
);
