create schema sigmausd;

/*
    Tables `bank_transactions` and `oracle_postings` store contract and oracle changes.
    Both have records retrievable by height to enable rollbacks.
    All other tables can be derived (and rolled back) using data from those two.
*/
create table sigmausd.bank_transactions (
    -- Bank box transaction index 
    idx integer primary key,
    -- Height of inclusion block
    height integer not null,
    -- Timestamp of inclusion block (required during rollbacks)
    timestamp bigint not null,
    -- Change in bank reserves (nanoERG)
    reserves_diff bigint not null,
    -- Change in circulating stable coins
    circ_sc_diff bigint not null,
    -- Change in circulating reserve coins
    circ_rc_diff bigint not null,
    -- Tx output box id
    box_id text unique not null,
    -- Service fee in nanoERG
    service_fee bigint not null,
    -- Address collecting the service fee, if any
    service_address_id bigint
);
create index on sigmausd.bank_transactions using brin(height);

create table sigmausd.oracle_postings (
    height integer primary key,
    datapoint bigint not null,
    box_id text not null
);

-- Transaction involving the bank box without affecting its balances.
-- Example: https://explorer.ergoplatform.com/en/transactions/fb7947eb2627d85661f07cdf9489c0e82a6027d0ad4aaee679e2775347b3e441
create table sigmausd.noop_bank_transactions (
    height integer not null,
    tx_idx integer not null,
    tx_id text primary key,
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
    height integer primary key,
    oracle bigint not null,
    circ_sc bigint not null,
    circ_rc bigint not null,
    reserves bigint not null,
    -- net nanoERG into bank from SC transactions
    sc_nano_net bigint,
    -- net nanoERG into bank from RC transactions
    rc_nano_net bigint
);

/*
    Tracks usage of and fees accumulated by services (e.g. TokenJay, sigmausd.io, ...).
    Includes direct interaction with sigmausd contract as a special case.

    Roll backs are handled by recreating the table from scratch by querying
    the bank_transactions table.
*/
create table sigmausd.services (
    -- address id of service or null for direct interaction
    address_id bigint,
    -- total transactions to date
    tx_count bigint not null,
    -- first and last tx timestamps
    first_tx bigint not null,
    last_tx bigint not null,
    -- total fees accumulated to date (nanoERG)
    fees numeric not null,
    -- total nanoERG going in/out of bank through service
    volume numeric not null,
    -- some checks
    check(tx_count > 0),
    check(last_tx >= first_tx),
    check(fees >= 0),
    check(volume >= 0)
);
-- A simple unique constraint would still allow for multiple null id's.
-- Instead, map null id to 0 to allow just a single null.
create unique index sigmausd_services_unique_coalesce_address_id
    on sigmausd.services (coalesce(address_id, 0));

-- Daily OHLC data
create table sigmausd.rc_ohlc_daily (
    t date primary key,
    o bigint not null,
    h bigint not null,
    l bigint not null,
    c bigint not null
);

-- Weekly OHLC data
create table sigmausd.rc_ohlc_weekly (
    t date primary key,
    o bigint not null,
    h bigint not null,
    l bigint not null,
    c bigint not null
);

-- Monthly OHLC data
create table sigmausd.rc_ohlc_monthly (
    t date primary key,
    o bigint not null,
    h bigint not null,
    l bigint not null,
    c bigint not null
);

-- OHLC log tables for rollbacks.
-- Keeps last record generated by each (recent) block.
-- When hitting a rollback for height h, the log is checked for a record at h.
-- If one is found, the ohlc record with the same date is deleted as well
-- as the log record itself. The resulting latest log record is upserted to the
-- ohlc data table.
-- Older log records may get deleted periodically.

-- Daily OHLC log
create table sigmausd._log_rc_ohlc_daily (
    height int primary key,
    t date not null,
    o bigint not null,
    h bigint not null,
    l bigint not null,
    c bigint not null
);

-- Weekly OHLC log
create table sigmausd._log_rc_ohlc_weekly (
    height int primary key,
    t date not null,
    o bigint not null,
    h bigint not null,
    l bigint not null,
    c bigint not null
);

-- Monthly OHLC log
create table sigmausd._log_rc_ohlc_monthly (
    height int primary key,
    t date not null,
    o bigint not null,
    h bigint not null,
    l bigint not null,
    c bigint not null
);


-----------------------------------------------------------------------------------------
-- Initialize state
-----------------------------------------------------------------------------------------
insert into ew.headers (schema_name, worker_id, height, timestamp, header_id, parent_id)
values (
    'sigmausd', -- must match the schema name declared in sigmausd worker
    'sigmausd', -- must match the WORKER_ID const declared in sigmausd worker
    453064,
    1616706545437,
    'fd35b157811f0950169e0f86b8f7e9ae0f13c49a46848ff40aa8dad26b030fde',
    '6fbf04c19bf97a558b0254cd09f77813b91cd5cdb40d22613bb8512046924dbd'
);

insert into sigmausd.history (
    height,
    oracle,
    circ_sc,
    circ_rc,
    reserves,
    sc_nano_net,
    rc_nano_net
) values (
    453064, -- height
    0, -- oracle, circ_sc is zero anyways
    0, -- circ_sc
    0, -- circ_rc
    1000000, -- reserves: 0.001 ERG
    0, -- sc_nano_net
    0 -- rc_nano_net
);

-- Default SigRSV ratio is 1 ERG = 1000000 SigRSV
insert into sigmausd.rc_ohlc_daily (t, o, h, l, c)
values (
    '2021-03-25'::date,
    1000000,
    1000000,
    1000000,
    1000000
);
insert into sigmausd.rc_ohlc_weekly (t, o, h, l, c)
values (
    '2021-03-22'::date,
    1000000,
    1000000,
    1000000,
    1000000
);
insert into sigmausd.rc_ohlc_monthly (t, o, h, l, c)
values (
    '2021-03-01'::date,
    1000000,
    1000000,
    1000000,
    1000000
);  
