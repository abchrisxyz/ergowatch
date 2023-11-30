create schema if not exists erg;

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

