create schema orp;

-------------------------------------------------------------------------------
-- Oracle Pools
-------------------------------------------------------------------------------
create table orp.pools
(
    id integer primary key,
    name text,
    datapoint_address text not null,
    epoch_prep_address text not null,
    live_epoch_address text not null,
    deposits_address text not null,
    pool_nft_id text not null,
    participant_token_id text not null,
    deviation_range integer not null,
    consensus_number integer not null
);


create table orp.oracles
(
    pool_id integer not null references orp.pools(id),
    oracle_id integer not null,
    address text unique not null,
    address_hash text unique not null,
    primary key(pool_id, oracle_id)
);


/*
    Dedicated relations for ERG/USD oracle pool.
    Can change when more oracles appear.
*/
create table orp.ergusd_prep_txs(
    tx_id text primary key,
    inclusion_height integer not null,
    datapoint integer not null,
    collector_id integer not null
);


create procedure orp.ergusd_update_prep_txs() AS
    $$
    with live_epoch_boxes as (
        select box_id
            , additional_registers #>>  '{R5,renderedValue}' as epoch_end_height
        from node_outputs
        where main_chain
            and address = (select live_epoch_address from orp.pools where id = 1)
            -- Limit to new boxes only
            and creation_height >= (select coalesce(max(inclusion_height), 0) from orp.ergusd_prep_txs)
    ), prep_txs as (
        select os.tx_id
            , os.settlement_height as inclusion_height
            , (os.additional_registers #>>  '{R4,renderedValue}')::integer as datapoint
        from live_epoch_boxes lep
        join node_inputs ins on ins.box_id = lep.box_id
        join node_outputs os on os.tx_id = ins.tx_id
        where ins.main_chain and os.main_chain
        -- Prep txs first output is the new prep box
        and os.address = (select epoch_prep_address from orp.pools where id = 1)
        -- Prep txs have only 1 input box: the live epoch box
        and ins.index = 0
    ), add_collector_hash as (
        -- Prep tx's second output's R4 is the index of the data input box
        -- belonging to the collecting oracle.
        -- Each datapoint box contains its oracle's address hash in R4.
        select prp.*
            , (dos.additional_registers #>> '{R4,renderedValue}') as collector_address_hash
        from prep_txs prp
        join node_outputs os1 on os1.tx_id = prp.tx_id and os1.index = 1
        join node_data_inputs din
            on din.tx_id = prp.tx_id
            and din.index = (os1.additional_registers #>> '{R4,renderedValue}')::integer
        join node_outputs dos on dos.box_id = din.box_id
        where os1.main_chain and din.main_chain and dos.main_chain
    )
    insert into orp.ergusd_prep_txs (tx_id, inclusion_height, datapoint, collector_id)
    select ach.tx_id
        , ach.inclusion_height
        , ach.datapoint
        , orc.oracle_id
    from add_collector_hash ach
    join orp.oracles orc
        on orc.address_hash = ach.collector_address_hash
    order by inclusion_height;
    $$ language sql;


create materialized view orp.ergusd_oracle_stats_mv as
    with datapoint_stats as (
        -- committed (not necessarily accepted) datapoints by oracle
        select nos.additional_registers #>> '{R4,renderedValue}' as oracle_address_hash
            , count(*) -1 nb_of_txs -- -1 to account for oracle creation tx
            , min(nos.timestamp) as first_ts
            , max(nos.timestamp) as last_ts
        from node_outputs nos
        where nos.main_chain
            and address = (select datapoint_address from orp.pools where id = 1)
        group by 1
        -- Discard oracles with only 1 tx, meaning they where created but never did anything.
        having count(*) > 1
    ), accepted_datapoint_stats as (
        select os.additional_registers #>> '{R4,renderedValue}' as oracle_address_hash
            , count(*) as payouts
            , min(os.timestamp) as first_ts
            , max(os.timestamp) as last_ts
        from orp.ergusd_prep_txs prp
        join node_data_inputs din on din.tx_id = prp.tx_id
        join node_outputs os on os.box_id = din.box_id
        where din.main_chain and os.main_chain
        group by 1
    ), collector_stats as (
        select prp.collector_id
            , count(*) as payouts
            , min(txs.timestamp) as first_ts
            , max(txs.timestamp) as last_ts
        from orp.ergusd_prep_txs prp
        join node_transactions txs on txs.id = prp.tx_id
        group by 1
    )
    -- Combine and convert address hash to address
    select orc.oracle_id
        , orc.address
        , coalesce(dat.nb_of_txs, 0) as commits
        , acc.payouts as accepted_commits
        , coalesce(col.payouts, 0) as collections
        , to_timestamp(dat.first_ts / 1000) as first_commit
        , to_timestamp(acc.first_ts / 1000) as first_accepted
        , to_timestamp(col.first_ts / 1000) as first_collection
        , to_timestamp(dat.last_ts / 1000) as last_commit
        , to_timestamp(acc.last_ts / 1000) as last_accepted
        , to_timestamp(col.last_ts / 1000) as last_collection
    from orp.oracles orc
    left join datapoint_stats dat
        on dat.oracle_address_hash = orc.address_hash
    left join accepted_datapoint_stats acc
        on acc.oracle_address_hash = orc.address_hash
    left join collector_stats col
        on col.collector_id = orc.oracle_id
    where orc.pool_id = 1
    order by orc.oracle_id
    with no data;


create materialized view orp.ergusd_latest_posting_mv as
    select inclusion_height as height
        , round(1. / prp.datapoint  * 1000000000, 2) as price
        , count(din.*) as datapoints
    from orp.ergusd_prep_txs prp
    join node_data_inputs din on din.tx_id = prp.tx_id
    where din.main_chain
    group by 1, 2
    order by prp.inclusion_height desc
    limit 1
    with no data;


-- number of blocks between successive price postings.
-- Only for recent epochs.
create materialized view orp.ergusd_recent_epoch_durations_mv as
    with indexed as (
        select row_number() over(order by inclusion_height) as idx
            , inclusion_height as height
        from orp.ergusd_prep_txs
        order by 1
    )
    select a.height
        , b.height - a.height as blocks
    from indexed a
    join indexed b on a.idx = b.idx - 1
    order by 1 desc
    limit 100
    with no data;


-- Enable concurrent refreshes
create unique index on orp.ergusd_oracle_stats_mv(oracle_id);
create unique index on orp.ergusd_latest_posting_mv(height);
create unique index on orp.ergusd_recent_epoch_durations_mv(height);

-- Initialize concurrent mv's (first call cannot be concurrentlty)
refresh materialized view orp.ergusd_latest_posting_mv;
refresh materialized view orp.ergusd_recent_epoch_durations_mv;
refresh materialized view orp.ergusd_oracle_stats_mv;


insert into orp.pools
(
    id,
    name,
    datapoint_address,
    epoch_prep_address,
    live_epoch_address,
    deposits_address,
    pool_nft_id,
    participant_token_id,
    deviation_range,
    consensus_number
)
values
(
    1,
    'ERGUSD',
    'AucEQEJ3Y5Uhmu4o8dsrHy28nRTgX5sVtXvjpMTqdMQzBR3uRVcvCFbv7SeGuPhQ16AXBP7XWdMShDdhRy4cayZgxHSkdAVuTiZRvj6WCfmhXJ4LY2E46CytRAnkiYubCdEroUUX2niMLhjNmDUn4KmXWSrKngrfGwHSaD8RJUMEp5AGADaChRU6kAnh9nstkDN3',
    'EfS5abyDe4vKFrJ48K5HnwTqa1ksn238bWFPe84bzVvCGvK1h2B7sgWLETtQuWwzVdBaoRZ1HcyzddrxLcsoM5YEy4UnqcLqMU1MDca1kLw9xbazAM6Awo9y6UVWTkQcS97mYkhkmx2Tewg3JntMgzfLWz5mACiEJEv7potayvk6awmLWS36sJMfXWgnEfNiqTyXNiPzt466cgot3GLcEsYXxKzLXyJ9EfvXpjzC2abTMzVSf1e17BHre4zZvDoAeTqr4igV3ubv2PtJjntvF2ibrDLmwwAyANEhw1yt8C8fCidkf3MAoPE6T53hX3Eb2mp3Xofmtrn4qVgmhNonnV8ekWZWvBTxYiNP8Vu5nc6RMDBv7P1c5rRc3tnDMRh2dUcDD7USyoB9YcvioMfAZGMNfLjWqgYu9Ygw2FokGBPThyWrKQ5nkLJvief1eQJg4wZXKdXWAR7VxwNftdZjPCHcmwn6ByRHZo9kb4Emv3rjfZE',
    'NTkuk55NdwCXkF1e2nCABxq7bHjtinX3wH13zYPZ6qYT71dCoZBe1gZkh9FAr7GeHo2EpFoibzpNQmoi89atUjKRrhZEYrTapdtXrWU4kq319oY7BEWmtmRU9cMohX69XMuxJjJP5hRM8WQLfFnffbjshhEP3ck9CKVEkFRw1JDYkqVke2JVqoMED5yxLVkScbBUiJJLWq9BSbE1JJmmreNVskmWNxWE6V7ksKPxFMoqh1SVePh3UWAaBgGQRZ7TWf4dTBF5KMVHmRXzmQqEu2Fz2yeSLy23sM3pfqa78VuvoFHnTFXYFFxn3DNttxwq3EU3Zv25SmgrWjLKiZjFcEcqGgH6DJ9FZ1DfucVtTXwyDJutY3ksUBaEStRxoUQyRu4EhDobixL3PUWRcxaRJ8JKA9b64ALErGepRHkAoVmS8DaE6VbroskyMuhkTo7LbrzhTyJbqKurEzoEfhYxus7bMpLTePgKcktgRRyB7MjVxjSpxWzZedvzbjzZaHLZLkWZESk1WtdM25My33wtVLNXiTvficEUbjA23sNd24pv1YQ72nY1aqUHa2',
    '4L1NEtpkMq6NeZhy2pk6omYvewovcHTm7CbxKm9djsbobAHSdDe6TVfmnW5THVpSHrG6rWovqv7838reswYE3UYkykWaNnhoyBGHFCdZvWqa2TVRtHiWcVaner6giUp55ZpELLuj9TtKePv6zrtMV5YE1o2kjrQEgGDoGHBGNuyx6ymXkSimcAQo1oD4f4tTcuBfWfp',
    '011d3364de07e5a26f0c4eef0852cddb387039a921b7154ef3cab22c6eda887f',
    '8c27dd9d8a35aac1e3167d58858c0a8b4059b277da790552e37eba22df9b9035',
    5,
    4
);

insert into orp.oracles (pool_id, oracle_id, address, address_hash) values
    -- ERGUSD oracles
    (1,  1, '9fPRvaMYzBPotu6NGvZn4A6N4J2jDmRGs4Zwc9UhFFeSXgRJ8pS', '02725e8878d5198ca7f5853dddf35560ddab05ab0a26adae7e664b84162c9962e5'),
    (1,  2, '9fQHnth8J6BgVNs9BQjxj5s4e5JGCjiH4fYTBA52ZWrMh6hz2si', '0274524ee849e4e45f58c46164ac609902bb374fc9375f097ee1af2ef1152ab9bf'),
    (1,  3, '9hD4D5rAcTyMuw7eVSENfRBmdCZiz3cwmW8xSnoEvZ1H64rFGMn', '036234820eb840b9246442f022ed1ef15ac80f2c5ac28314bcd8ff682c2703128f'),
    (1,  4, '9fckoJSnYpR38EkCzintbJoKaDwWN86wCmNdByiWyeQ22Hq5Sbj', '0290a0538b85768adb3dfc1fe6e4162adf43c6ae313ada0d1a7b71275de2b87364'),
    (1,  5, '9fzRcctiWfzoJyqGtPWqoXPuxSmFw6zpnjtsQ1B6jSN514XqH4q', '02c1d434dac8765fc1269af82958d8aa350da53907096b35f7747cc372a7e6e69d'),
    (1,  6, '9eh9WDsRAsujyFx4x7YeSoxrLCqmhuQihDwgsWVqEuXte7QJRCU', '0216e6cca588bed47a7630cba9d662a4be8a2e1991a45ed54ba64093e03dcd9013'),
    (1,  7, '9gXPZWxQZQpKsBCW2QCiBjJbhtghxEFtA9Ba6WygnKmrD4g2e8B', '03082348fd5d0c27d7aa89cd460a58fea2932f12147a04985e500bd9ad64695d58'),
    (1,  8, '9g4Kek6iWspXPAURU3zxT4RGoKvFdvqgxgkANisNFbvDwK1KoxW', '02caad8ef6771ad15ebb0a2aa9b7e84b9c48962976061d1af3e73767203d2f2bb1'),
    (1,  9, '9eY1GWpJ7qwMkfVtnt8gZDnSvNW9VPqt15vePUmRrcr2zCRpGQ4', '020224bd8e95bb60ec042b5172d3cc9dd79f74f99700934010cda16642a50bd7af'),
    (1, 10, '9em1ShUCkTa43fALGgzwKQ5znuXY2dMnnfHqq4bX3wSWytH11t7', '021fab219a58d2e1e8edfd3e2ad7cf09a35687246c084477db0bce5412f43acdbe'),
    (1, 11, '9gqhrGQN3eQmTFAW9J6KNX8ffUhe8BmTesE45b9nBmL7VJohhtY', '0331b99a9fcc7bceb0a238446cdab944402dd4b2e79f9dcab898ec3b46aea285c8');


-------------------------------------------------------------------------------
-- Sync
-------------------------------------------------------------------------------
create table orp.sync_status (
    last_sync_height integer
);
insert into orp.sync_status (last_sync_height) values (0);


-- drop procedure if exists orp.sync;
create procedure orp.sync(in _height integer) as
    $$

    call orp.ergusd_update_prep_txs();
    refresh materialized view concurrently orp.ergusd_latest_posting_mv;
    refresh materialized view concurrently orp.ergusd_recent_epoch_durations_mv;
    refresh materialized view concurrently orp.ergusd_oracle_stats_mv;

    -- Sync Status
    update orp.sync_status
    set last_sync_height = _height;

    $$ language sql;
