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
comment on table erg._rev is 'Current schema revision'

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
    check (value > 0)
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
