create schema sigmausd;

-- Last processed block
create table sigmausd.head (
    singleton int primary key default 1,
    height integer not null,
    header_id text not null,
    check(singleton = 1)
);
insert into sigmausd.head(height, header_id) values (-1, '');

/*
    Tables `bank_transactions` and `oracle_postings` store contract and oracle changes.
    Both have records retrievable by height to enable roll backs.
    All other tables can be derived (and rolled back) using data from those two.
*/
create table sigmausd.bank_transactions (
    -- Bank box transaction index 
    idx integer primary key,
    height integer not null,
    -- Change in bank reserves (nanoERG)
    reserves_diff bigint not null,
    -- Change in circulating stable coins
    sc_diff bigint not null,
    -- Change in circulating reserve coins
    rc_diff bigint not null,
    -- Tx output box id
    box_id text unique not null,
    -- Service fee in nanoERG
    service_fee bigint not null,
    -- Address collecting the service fee, if any
    service_address_id bigint
);
create index on sigmausd.bank_transactions using brin(height);

create table sigmausd.oracle_postings (
    height integer primary key not null,
    datapoint bigint not null,
    box_id text not null
);

/*
    Combined bank and oracle history.
    Reserve state (circ SC, circ RC, reserves) and oracle price
    at any height with a sigmausd tx and/or oracle price change.
    Liabilities, RSV price, equity and RR can all be derived from
    reserve state and oracle price, so not included.
    Shows state after last change in block.
*/
create table sigmausd.history (
    height integer primary key not null,
    oracle bigint not null,
    circ_sc bigint not null,
    circ_rc bigint not null,
    reserves bigint not null,
    -- track erg in - erg out from sc and rc transactions
    -- can always be derived from bank_transactions...
    -- sc_nano_net bigint not null
    -- rc_nano_net bigint not null
);

-- 
create table sigmausd.totals (
    -- minted SC
    sc_minted numeric,
    -- redeemed SC
    sc_redeemed numeric,
    -- nanoERG into bank from minting SC
    sc_nano_in numeric,
    -- nanoERG out of bank from redeeming SC
    sc_nano_out numeric,
    -- minted RC
    rc_minted numeric,
    -- redeemed RC
    rc_redeemed numeric,
    -- nanoERG into bank from minting RC
    rc_nano_in numeric,
    -- nanoERG out of bank from redeeming RC
    rc_nano_out numeric,
    -- singleton
    singleton int primary key default 1,
    check(singleton = 1)

    -- OR, maintain ROI's directly...
    -- net nanoERG into bank from SC transaction
    sc_nano_net

    -- SC roi = liabilities / sc_nano_net - 1
);


/*
    Tracks usage of and fees accumulated by services (e.g. TokenJay, sigmausd.io, ...).
    Includes direct interaction with sigmausd contract as a special case.
*/
create table sigmausd.services (
    -- address id of service or null for direct interaction
    address_id integer primary key,
    -- total transactions to date
    tx_count bigint not null,
    -- first and last tx timestamps
    first_tx timestamp not null,
    last_tx timestamp not null,
    -- total fees accumulated to date (nanoERG)
    fees numeric not null,
    -- total nanoERG going in/out of bank through service
    volume numeric not null,
    -- some checks
    check(tx_count > 0),
    check(lat_tx >= first_tx),
    check(fees >= 0),
    check(volume >= 0)
);

-- Convenience view for debugging?
create view sigmausd.details as
    select 1; -- todo



