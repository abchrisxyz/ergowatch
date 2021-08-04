-- begin;

drop schema if exists ew cascade;
create schema ew;

-------------------------------------------------------------------------------
-- Oracle Pools
-------------------------------------------------------------------------------
create table ew.oracle_pools
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


create table ew.oracle_pools_oracles
(
    pool_id integer not null references ew.oracle_pools(id),
    oracle_id integer not null,
    address text unique not null,
    address_hash text unique not null,
    primary key(pool_id, oracle_id)
);


/*
    Dedicated relations for ERG/USD oracle pool.
    Can change when more oracles appear.
*/
create table ew.oracle_pools_ergusd_prep_txs(
    tx_id text primary key,
    inclusion_height integer not null,
    datapoint integer not null,
    collector_id integer not null
);


create procedure ew.oracle_pools_ergusd_update_prep_txs() AS
    $$
    with live_epoch_boxes as (
        select box_id
            , additional_registers #>>  '{R5,renderedValue}' as epoch_end_height
        from node_outputs
        where main_chain
            and address = (select live_epoch_address from ew.oracle_pools where id = 1)
            -- Limit to new boxes only
            and creation_height >= (select coalesce(max(inclusion_height), 0) from ew.oracle_pools_ergusd_prep_txs)
    ), prep_txs as (
        select os.tx_id
            , os.settlement_height as inclusion_height
            , (os.additional_registers #>>  '{R4,renderedValue}')::integer as datapoint
        from live_epoch_boxes lep
        join node_inputs ins on ins.box_id = lep.box_id
        join node_outputs os on os.tx_id = ins.tx_id
        where ins.main_chain and os.main_chain
        -- Prep txs first output is the new prep box
        and os.address = (select epoch_prep_address from ew.oracle_pools where id = 1)
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
    insert into ew.oracle_pools_ergusd_prep_txs (tx_id, inclusion_height, datapoint, collector_id)
    select ach.tx_id
        , ach.inclusion_height
        , ach.datapoint
        , orc.oracle_id
    from add_collector_hash ach
    join ew.oracle_pools_oracles orc
        on orc.address_hash = ach.collector_address_hash
    order by inclusion_height;
    $$ language sql;


create materialized view ew.oracle_pools_ergusd_oracle_stats_mv as
    with datapoint_stats as (
        -- committed (not necessarily accepted) datapoints by oracle
        select nos.additional_registers #>> '{R4,renderedValue}' as oracle_address_hash
            , count(*) -1 nb_of_txs -- -1 to account for oracle creation tx
            , min(nos.timestamp) as first_ts
            , max(nos.timestamp) as last_ts
        from node_outputs nos
        where nos.main_chain
            and address = (select datapoint_address from ew.oracle_pools where id = 1)
        group by 1
		-- Discard oracles with only 1 tx, meaning they where created but never did anything.
		having count(*) > 1
    ), accepted_datapoint_stats as (
        select os.additional_registers #>> '{R4,renderedValue}' as oracle_address_hash
            , count(*) as payouts
            , min(os.timestamp) as first_ts
            , max(os.timestamp) as last_ts
        from ew.oracle_pools_ergusd_prep_txs prp
        join node_data_inputs din on din.tx_id = prp.tx_id
        join node_outputs os on os.box_id = din.box_id
        where din.main_chain and os.main_chain
        group by 1
    ), collector_stats as (
        select prp.collector_id
            , count(*) as payouts
            , min(txs.timestamp) as first_ts
            , max(txs.timestamp) as last_ts
        from ew.oracle_pools_ergusd_prep_txs prp
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
    from ew.oracle_pools_oracles orc
    left join datapoint_stats dat
        on dat.oracle_address_hash = orc.address_hash
    left join accepted_datapoint_stats acc
        on acc.oracle_address_hash = orc.address_hash
    left join collector_stats col
        on col.collector_id = orc.oracle_id
    where orc.pool_id = 1
	order by orc.oracle_id
    with no data;
	

create materialized view ew.oracle_pools_ergusd_latest_posting_mv as
	select inclusion_height as height
		, round(1. / prp.datapoint  * 1000000000, 2) as price
		, count(din.*) as datapoints
	from ew.oracle_pools_ergusd_prep_txs prp
	join node_data_inputs din on din.tx_id = prp.tx_id
	where din.main_chain
	group by 1, 2
	order by prp.inclusion_height desc
	limit 1
	with no data;
	

-- number of blocks between successive price postings.
-- Only for recent epochs.
create materialized view ew.oracle_pools_ergusd_recent_epoch_durations_mv as
	with indexed as (
		select row_number() over(order by inclusion_height) as idx
			, inclusion_height as height
		from ew.oracle_pools_ergusd_prep_txs
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
create unique index on ew.oracle_pools_ergusd_oracle_stats_mv(oracle_id);
create unique index on ew.oracle_pools_ergusd_latest_posting_mv(height);
create unique index on ew.oracle_pools_ergusd_recent_epoch_durations_mv(height);

-- Initialize concurrent mv's (first call cannot be concurrentlty)
refresh materialized view ew.oracle_pools_ergusd_latest_posting_mv;
refresh materialized view ew.oracle_pools_ergusd_recent_epoch_durations_mv;
refresh materialized view ew.oracle_pools_ergusd_oracle_stats_mv;


insert into ew.oracle_pools
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


insert into ew.oracle_pools_oracles (pool_id, oracle_id, address, address_hash) values
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
-- SigmaUSD
-------------------------------------------------------------------------------
create table ew.sigmausd_bank_boxes
(
    idx integer primary key,
    box_id text unique not null
);

create procedure ew.sigmausd_update_bank_boxes()
    language sql
    as $$
        insert into ew.sigmausd_bank_boxes (idx, box_id)
            with recursive box_series(idx, box_id) as (
                -- start from latest known bank box
                select idx, box_id
                from (
                    select idx, box_id from ew.sigmausd_bank_boxes bbs
                    order by idx DESC
                    LIMIT 1
                ) last_bank_box

                union

                select idx+1 as idx
                    , os.box_id
                from box_series bxs
                join node_inputs i on i.box_id = bxs.box_id
                join node_outputs os on os.tx_id = i.tx_id and os.address = 'MUbV38YgqHy7XbsoXWF5z7EZm524Ybdwe5p9WDrbhruZRtehkRPT92imXer2eTkjwPDfboa1pR3zb3deVKVq3H7Xt98qcTqLuSBSbHb7izzo5jphEpcnqyKJ2xhmpNPVvmtbdJNdvdopPrHHDBbAGGeW7XYTQwEeoRfosXzcDtiGgw97b2aqjTsNFmZk7khBEQywjYfmoDc9nUCJMZ3vbSspnYo3LarLe55mh2Np8MNJqUN9APA6XkhZCrTTDRZb1B4krgFY1sVMswg2ceqguZRvC9pqt3tUUxmSnB24N6dowfVJKhLXwHPbrkHViBv1AKAJTmEaQW2DN1fRmD9ypXxZk8GXmYtxTtrj3BiunQ4qzUCu1eGzxSREjpkFSi2ATLSSDqUwxtRz639sHM6Lav4axoJNPCHbY8pvuBKUxgnGRex8LEGM8DeEJwaJCaoy8dBw9Lz49nq5mSsXLeoC4xpTUmp47Bh7GAZtwkaNreCu74m9rcZ8Di4w1cmdsiK1NWuDh9pJ2Bv7u3EfcurHFVqCkT3P86JUbKnXeNxCypfrWsFuYNKYqmjsix82g9vWcGMmAcu5nagxD4iET86iE2tMMfZZ5vqZNvntQswJyQqv2Wc6MTh4jQx1q2qJZCQe4QdEK63meTGbZNNKMctHQbp3gRkZYNrBtxQyVtNLR8xEY8zGp85GeQKbb37vqLXxRpGiigAdMe3XZA4hhYPmAAU5hpSMYaRAjtvvMT3bNiHRACGrfjvSsEG9G2zY5in2YWz5X9zXQLGTYRsQ4uNFkYoQRCBdjNxGv6R58Xq74zCgt19TxYZ87gPWxkXpWwTaHogG1eps8WXt8QzwJ9rVx6Vu9a5GjtcGsQxHovWmYixgBU8X9fPNJ9UQhYyAWbjtRSuVBtDAmoV1gCBEPwnYVP5GCGhCocbwoYhZkZjFZy6ws4uxVLid3FxuvhWvQrVEDYp7WRvGXbNdCbcSXnbeTrPMey1WPaXX'
            )
            select idx, box_id
            from box_series
            on conflict do nothing;
    $$;

-- First SigmaUSD bank box
insert into ew.sigmausd_bank_boxes(idx, box_id)
values (0, '96dec7ca2812ee8bb6c0e1969aef383f2ee4f79510c587e83f3ac59a0aff1678');

/*
SigmaUSD history
----------------
The history is split across three tables:

    - Bank tx history (reserves, circulating supplies, accumulated feed)
      Changes with bank txs only.

    - cumulative bank tx history (total fees, ...)

    - Bank ratio's history (liabilities, equity, SigUSD and SigRSV prices)
      Change with bank txs and oracle postings.

Tx history only includes heights where at least 1 bank tx is mined.
Tx history can have more than 1 record (tx) per height.
Tx and cumulative tx history is split across two tables for easier updates.

Ratio history only includes heights where:
    a) oracle price is posted, or
    b) one or more bank txs are mined
Ratio history has only 1 record per height (only the last tx matters).
*/

-- SigmaUSD bank transactions
create table ew.sigmausd_history_transactions (
    bank_box_idx integer primary key references ew.sigmausd_bank_boxes(idx),
    height integer,
    reserves numeric,
    circ_sigusd numeric,
    circ_sigrsv bigint,
    d_erg numeric,
    d_usd numeric,
    d_rsv bigint,
    fee numeric,
    check(fee >= 0)
);
create index on ew.sigmausd_history_transactions(height);


-- SigmaUSD bank cumulative stats
create table ew.sigmausd_history_transactions_cumulative (
    bank_box_idx integer primary key references ew.sigmausd_bank_boxes(idx),
    cum_usd_fee numeric,
    cum_rsv_fee numeric,
    cum_usd_erg_in numeric,
    cum_usd_erg_out numeric,
    cum_rsv_erg_in numeric,
    cum_rsv_erg_out numeric,
    f_usd numeric,
    f_rsv numeric,
    f_fee numeric
);


create table ew.sigmausd_history_ratios (
    height integer primary key,
    oracle_price numeric,
    rsv_price numeric,
    liabs numeric,
    equity numeric
);

-- Inserts new records in sigmausd history tables
create procedure ew.sigmausd_update_history()
    as $$
        --------------------------------
        -- 1. Update transaction history
        --------------------------------
        with last_processed_bank_box as (
            select coalesce(max(bank_box_idx), -1) as idx
            from ew.sigmausd_history_transactions
        ), bank_boxes as (
            select bbs.idx
                , nos.creation_height
                , nos.settlement_height
                , ROUND(nos.value / 1000000000., 9) as reserves
                , ROUND((nos.additional_registers #>> '{R4,renderedValue}')::numeric / 100, 2) as circ_sigusd
                , (nos.additional_registers #>> '{R5,renderedValue}')::bigint as circ_sigrsv
            from ew.sigmausd_bank_boxes bbs
            join node_outputs nos on nos.box_id = bbs.box_id and nos.main_chain
            -- Limit to new boxes only,
            -- but include last processed box so we can derive
            -- changes for first new box as well.
            where bbs.idx >= (select idx from last_processed_bank_box)
        ), ergusd_oracle_pool_price_boxes as (
            -- Retrieve oracle price postings (prep boxes).
            select nos.settlement_height
                , 1 / (nos.additional_registers #>> '{R4,renderedValue}')::numeric * 1000000000 as price
            from ew.oracle_pools_ergusd_prep_txs prp
            join node_outputs nos on nos.tx_id = prp.tx_id
			join node_transactions nts on nts.id = prp.tx_id
            where nos.main_chain and nts.main_chain and nos.index = 0
                -- Oldest oracle box that we need is the one that existed when
                -- the oldest bank box was created.
                and nts.inclusion_height >= (select min(creation_height) from bank_boxes)
        ), combined_bank_boxes_and_oracle_prices as (
            select bbs.*
                -- Highest oracle height under or at bank tx creation height
                , (
                    select settlement_height
                    from ergusd_oracle_pool_price_boxes
                    where settlement_height <= bbs.creation_height
                    order by settlement_height DESC LIMIT 1
                ) as oracle_height
                , (
                    select price
                    from ergusd_oracle_pool_price_boxes
                    where settlement_height <= bbs.creation_height
                    order by settlement_height DESC LIMIT 1
                ) as oracle_price
            from bank_boxes bbs
        ), add_diffs as (
            select a.*
                , a.reserves - b.reserves as d_erg
                , a.circ_sigusd - b.circ_sigusd as d_usd
                , a.circ_sigrsv - b.circ_sigrsv as d_rsv
            from combined_bank_boxes_and_oracle_prices a
            left join combined_bank_boxes_and_oracle_prices b on b.idx = a.idx - 1
        ), add_liabs as (
            select *
                , (circ_sigusd - d_usd) / oracle_price as old_liabs
                , circ_sigusd / oracle_price as liabs
            from add_diffs
        ), add_equity as (
            select *
                , (reserves - d_erg) - old_liabs as old_equity
                , reserves - liabs as equity
            from add_liabs
        ), add_rsv_price as (
            select *
                , case when (circ_sigrsv - d_rsv) > 0 then old_equity / (circ_sigrsv - d_rsv) else 0.001 end as old_rsv_price
                , case when circ_sigrsv > 0 then equity / circ_sigrsv else 0.001 end as rsv_price
            from add_equity
        ), add_fee as (
            select *
                , d_erg - (d_usd / oracle_price) - (d_rsv * old_rsv_price)  as fee
            from add_rsv_price
        )
        insert into ew.sigmausd_history_transactions
        (
            bank_box_idx,
            height,
            reserves,
            circ_sigusd,
            circ_sigrsv,
            d_erg,
            d_usd,
            d_rsv,
            fee
        )
        select idx
            , settlement_height
            , reserves
            , circ_sigusd
            , circ_sigrsv
            , d_erg
            , d_usd
            , d_rsv
            , fee
        from add_fee
        -- Ignore first idx as already processed or initial bank box
        order by idx offset 1;

        -------------------------------------------
        -- 2. Update cumulative transaction history
        -------------------------------------------
        with cumsum_for_new_boxes as (
            select th.bank_box_idx
                , reserves
                , sum(fee * (d_usd <> 0)::int) over w as cum_usd_fee
                , sum(fee * (d_rsv <> 0)::int) over w as cum_rsv_fee

                , sum(greatest(0,d_erg) * (d_usd > 0)::int) over w as cum_usd_erg_in
                , sum(greatest(0,-d_erg) * (d_usd < 0)::int) over w as cum_usd_erg_out

                , sum(greatest(0,d_erg) * (d_rsv > 0)::int) over w as cum_rsv_erg_in
                , sum(greatest(0,-d_erg) * (d_rsv < 0)::int) over w as cum_rsv_erg_out
            from ew.sigmausd_history_transactions th
            left join ew.sigmausd_history_transactions_cumulative ch on ch.bank_box_idx = th.bank_box_idx
            where ch.bank_box_idx is null
            window w as (order by th.bank_box_idx)
        ), adjusted_cumsums as (
            -- Add cumsum form last record.
            -- coalesce to 0 for initial run.
            select nbs.bank_box_idx
                , nbs.reserves
                , nbs.cum_usd_fee + coalesce(lcr.cum_usd_fee, 0) as cum_usd_fee
                , nbs.cum_rsv_fee + coalesce(lcr.cum_rsv_fee, 0) as cum_rsv_fee
                , nbs.cum_usd_erg_in + coalesce(lcr.cum_usd_erg_in, 0) as cum_usd_erg_in
                , nbs.cum_usd_erg_out + coalesce(lcr.cum_usd_erg_out, 0) as cum_usd_erg_out
                , nbs.cum_rsv_erg_in + coalesce(lcr.cum_rsv_erg_in, 0) as cum_rsv_erg_in
                , nbs.cum_rsv_erg_out + coalesce(lcr.cum_rsv_erg_out, 0) as cum_rsv_erg_out
            from cumsum_for_new_boxes nbs
            -- Last cumulative record
            left join (select * from ew.sigmausd_history_transactions_cumulative order by bank_box_idx DESC LIMIT 1) lcr on TRUE
        ), add_reserve_fractions as (
            select *
                , greatest(0, cum_usd_erg_in - cum_usd_erg_out - cum_usd_fee) / reserves as f_usd
                , (cum_rsv_erg_in - cum_rsv_erg_out - cum_rsv_fee + 0.001 + least(0, cum_usd_erg_in - cum_usd_erg_out - cum_usd_fee)) / reserves as f_rsv
                , (cum_usd_fee + cum_rsv_fee) / reserves as f_fee
            from adjusted_cumsums
        )
        insert into ew.sigmausd_history_transactions_cumulative
        (
            bank_box_idx,
            cum_usd_fee,
            cum_rsv_fee,
            cum_usd_erg_in,
            cum_usd_erg_out,
            cum_rsv_erg_in,
            cum_rsv_erg_out,
            f_usd,
            f_rsv,
            f_fee
        )
        select bank_box_idx
            , cum_usd_fee
            , cum_rsv_fee
            , cum_usd_erg_in
            , cum_usd_erg_out
            , cum_rsv_erg_in
            , cum_rsv_erg_out
            , f_usd
            , f_rsv
            , f_fee
        from add_reserve_fractions
        order by bank_box_idx;

        ----------------------------
        -- 3. Update ratio's history
        ----------------------------
        -- For this one we first need to collect all new oracle price heights as
        -- well as any new bank tx heights.
        -- Then add oracle price and bank state for each new height.
        with new_bank_transactions as (
			select bank_box_idx
				, height
			from ew.sigmausd_history_transactions
			-- Limit to new heights only,
			where height > (select coalesce(max(height), -1) from ew.sigmausd_history_ratios)
				-- Only keep last box within each block (txs within a block have same timestamp)
				and (height, bank_box_idx) in (
						select height, max(bank_box_idx)
						from ew.sigmausd_history_transactions
						where height > (select coalesce(max(height), -1) from ew.sigmausd_history_ratios)
						group by 1
					)
		), new_ergusd_oracle_pool_price_boxes as (
			-- Retrieve oracle price postings (prep boxes).
			select inclusion_height as height
			from ew.oracle_pools_ergusd_prep_txs
			-- Limit to new heights only
			where inclusion_height > (select coalesce(max(height), -1) from ew.sigmausd_history_ratios)
		), new_heights_combined as (
			select coalesce(op.height, bt.height) as height
			from new_ergusd_oracle_pool_price_boxes op
			full outer join new_bank_transactions bt on bt.height = op.height
		), add_oracle_prices as (
			-- For each height, get the price posted at that height,
			-- or the first one before that.
			select hs.height
				, 1. / op.datapoint * 1000000000 as oracle_price
		     	-- , op.inclusion_height
			from new_heights_combined hs
			left join ew.oracle_pools_ergusd_prep_txs op on op.inclusion_height <= hs.height
			where (hs.height, op.inclusion_height) in (
					select hs.height
						, max(op.inclusion_height)
					from new_heights_combined hs
					left join ew.oracle_pools_ergusd_prep_txs op on op.inclusion_height <= hs.height
					group by 1
				)		
		), add_bank_state as (
			-- For each height, get the bank state from bank tx at that height,
			-- or the first one before that.
			select wrk.height
				, wrk.oracle_price
				, htx.reserves
				, htx.circ_sigusd
				, htx.circ_sigrsv
		     	-- , wrk.inclusion_height, htx.height, htx.bank_box_idx
			from add_oracle_prices wrk
			left join ew.sigmausd_history_transactions htx on htx.height <= wrk.height
			where (wrk.height, htx.height) in (
					select wrk.height, max(htx.height)
					from add_oracle_prices wrk
					left join ew.sigmausd_history_transactions htx on htx.height <= wrk.height
					group by 1
				)
				-- Limit to bank txs that we need.
				-- Oldest bank txs needed were in last block of tx history prior to last tx history update.
				-- To find it we intersect the (now updated) tx history with the (not yet updated)
				-- ratio history to find the latest common block.
				and htx.height >= (
					select t.height
					from ew.sigmausd_history_transactions t
					join ew.sigmausd_history_ratios r on r.height = t.height
					-- add 0 in case ratio history is empty
					union select 0 as height
					order by 1 desc
					limit 1
				)
				-- Only keep last box within each block (txs within a block have same timestamp)
				and (htx.height, htx.bank_box_idx) in (
					select height, max(bank_box_idx)
					from ew.sigmausd_history_transactions
					group by 1
				)
		), add_liabs as (
			select *
				, circ_sigusd / oracle_price as liabs
			from add_bank_state
		), add_equity as (
			select *
				, reserves - liabs as equity
			from add_liabs
		), add_rsv_price as (
			select *
				, case when circ_sigrsv > 0 then equity / circ_sigrsv else 0.001 end as rsv_price
			from add_equity
		)
		insert into ew.sigmausd_history_ratios
		(
			height,
			oracle_price,
			rsv_price,
			liabs,
			equity
		)
		select height
			, oracle_price
			, rsv_price
			, liabs
			, equity
		from add_rsv_price
		order by height;
    $$
    language sql;
	
	
-------------------------------------------------------------------------------
-- Sync
-------------------------------------------------------------------------------
create table ew.sync_status (
	last_sync_height integer
);
insert into ew.sync_status (last_sync_height) values (0);

create function ew.notify_new_header() returns trigger as
	$$
	begin
	perform pg_notify('ergowatch', (select height::text from new_table));
	return null;
	end;
	$$ language plpgsql;
	
create trigger notify_node_headers_insert
    AFTER INSERT on node_headers
    REFERENCING NEW TABLE as new_table
    FOR EACH STATEMENT
    EXECUTE FUNCTION ew.notify_new_header();


-- drop procedure if exists ew.sync;
create procedure ew.sync(in _height integer) as
	$$
	
	-- Oracle Pools
	call ew.oracle_pools_ergusd_update_prep_txs();
	refresh materialized view concurrently ew.oracle_pools_ergusd_latest_posting_mv;
	refresh materialized view concurrently ew.oracle_pools_ergusd_recent_epoch_durations_mv;
	refresh materialized view concurrently ew.oracle_pools_ergusd_oracle_stats_mv;
	
	-- SigmaUSD
	call ew.sigmausd_update_bank_boxes();
	call ew.sigmausd_update_history();
	
	-- Sync Status
	update ew.sync_status
	set last_sync_height = _height;

	$$ language sql;


-- commit;