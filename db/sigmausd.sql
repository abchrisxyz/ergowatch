create schema sig;


-------------------------------------------------------------------------------
-- SigmaUSD
-------------------------------------------------------------------------------
create table sig.bank_boxes
(
    idx integer primary key,
    box_id text unique not null
);

create procedure sig.update_bank_boxes()
    language sql
    as $$
        insert into sig.bank_boxes (idx, box_id)
            with recursive box_series(idx, box_id) as (
                -- start from latest known bank box
                select idx, box_id
                from (
                    select idx, box_id from sig.bank_boxes bbs
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
insert into sig.bank_boxes(idx, box_id)
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
create table sig.history_transactions (
    bank_box_idx integer primary key references sig.bank_boxes(idx),
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
create index on sig.history_transactions(height);


-- SigmaUSD bank cumulative stats
create table sig.history_transactions_cumulative (
    bank_box_idx integer primary key references sig.bank_boxes(idx),
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


create table sig.history_ratios (
    height integer primary key,
    oracle_price numeric,
    rsv_price numeric,
    liabs numeric,
    equity numeric
);

-- Inserts new records in sigmausd history tables
create procedure sig.update_history()
    as $$
        --------------------------------
        -- 1. Update transaction history
        --------------------------------
        with last_processed_bank_box as (
            select coalesce(max(bank_box_idx), -1) as idx
            from sig.history_transactions
        ), bank_boxes as (
            select bbs.idx
                , nos.creation_height
                , nos.settlement_height
                , ROUND(nos.value / 1000000000., 9) as reserves
                , ROUND((nos.additional_registers #>> '{R4,renderedValue}')::numeric / 100, 2) as circ_sigusd
                , (nos.additional_registers #>> '{R5,renderedValue}')::bigint as circ_sigrsv
            from sig.bank_boxes bbs
            join node_outputs nos on nos.box_id = bbs.box_id and nos.main_chain
            -- Limit to new boxes only,
            -- but include last processed box so we can derive
            -- changes for first new box as well.
            where bbs.idx >= (select idx from last_processed_bank_box)
        ), ergusd_oracle_pool_price_boxes as (
            -- Retrieve oracle price postings (prep boxes).
            select nos.settlement_height
                , 1 / (nos.additional_registers #>> '{R4,renderedValue}')::numeric * 1000000000 as price
            from orp.ergusd_prep_txs prp
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
        insert into sig.history_transactions
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
            from sig.history_transactions th
            left join sig.history_transactions_cumulative ch on ch.bank_box_idx = th.bank_box_idx
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
            left join (select * from sig.history_transactions_cumulative order by bank_box_idx DESC LIMIT 1) lcr on TRUE
		), add_reserve_fractions as (
            select *
                , greatest(0, cum_usd_erg_in - cum_usd_erg_out - cum_usd_fee) / reserves as f_usd
                , (cum_rsv_erg_in - cum_rsv_erg_out - cum_rsv_fee + 0.001 + least(0, cum_usd_erg_in - cum_usd_erg_out - cum_usd_fee)) / reserves as f_rsv
                , (cum_usd_fee + cum_rsv_fee) / reserves as f_fee
            from adjusted_cumsums
        )
        insert into sig.history_transactions_cumulative
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
			from sig.history_transactions
			-- Limit to new heights only,
			where height > (select coalesce(max(height), -1) from sig.history_ratios)
				-- Only keep last box within each block (txs within a block have same timestamp)
				and (height, bank_box_idx) in (
						select height, max(bank_box_idx)
						from sig.history_transactions
						where height > (select coalesce(max(height), -1) from sig.history_ratios)
						group by 1
					)
		), new_ergusd_oracle_pool_price_boxes as (
			-- Retrieve oracle price postings (prep boxes).
			select inclusion_height as height
			from orp.ergusd_prep_txs
			-- Limit to new heights only
			where inclusion_height > (select coalesce(max(height), -1) from sig.history_ratios)
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
			left join orp.ergusd_prep_txs op on op.inclusion_height <= hs.height
			where (hs.height, op.inclusion_height) in (
					select hs.height
						, max(op.inclusion_height)
					from new_heights_combined hs
					left join orp.ergusd_prep_txs op on op.inclusion_height <= hs.height
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
			left join sig.history_transactions htx on htx.height <= wrk.height
			where (wrk.height, htx.height) in (
					select wrk.height, max(htx.height)
					from add_oracle_prices wrk
					left join sig.history_transactions htx on htx.height <= wrk.height
					group by 1
				)
				-- Limit to bank txs that we need.
				-- Oldest bank txs needed were in last block of tx history prior to last tx history update.
				-- To find it we intersect the (now updated) tx history with the (not yet updated)
				-- ratio history to find the latest common block.
				and htx.height >= (
					select t.height
					from sig.history_transactions t
					join sig.history_ratios r on r.height = t.height
					-- add 0 in case ratio history is empty
					union select 0 as height
					order by 1 desc
					limit 1
				)
				-- Only keep last box within each block (txs within a block have same timestamp)
				and (htx.height, htx.bank_box_idx) in (
					select height, max(bank_box_idx)
					from sig.history_transactions
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
		insert into sig.history_ratios
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


/*------------------------------ SigmaUSD history series ----------------------------- */

-- Full sigmausd history
-- Reserve state (circ SC, circ RC, reserves) and oracle price
-- at every height with a sigmausd tx and/or oracle price change.
-- Liabilities, RSV price, equity and RR can all be derived from
-- reserve state and oracle price, so not included.
-- Also adding net erg balance for SC and RC from cummulative history.
create materialized view sig.series_history_mv as
    -- Collect all oracle price heights as well as all bank tx heights.
    -- Then add oracle price and bank state for each new height.
    with bank_transactions as (
        select bank_box_idx
            , height
        from sig.history_transactions
         where true
             -- Only keep last box within each block (txs within a block have same timestamp)
             and (height, bank_box_idx) in (
                     select height, max(bank_box_idx)
                     from sig.history_transactions
                     group by 1
                 )
    ), ergusd_oracle_pool_price_boxes as (
        -- Retrieve oracle price postings (prep boxes).
        select inclusion_height as height
            , 1. / datapoint * 1000000000 as oracle_price
        from orp.ergusd_prep_txs
        where inclusion_height >= 453480
    ), combined as (
        select coalesce(op.height, bt.height) as height
            , bt.bank_box_idx
            , op.oracle_price
        from ergusd_oracle_pool_price_boxes op
        full outer join bank_transactions bt on bt.height = op.height
    ), fill_blanks as (
        -- Add first previous bank_box_idx to heights without one
        -- and add first previous oracle price to heights without one.
        select height
            , first_value(bank_box_idx) over (partition by bb_rank order by height, bank_box_idx) as bank_box_idx
            , first_value(oracle_price) over (partition by op_rank order by height, bank_box_idx) as oracle_price
        from (
            select height
            , bank_box_idx
            , oracle_price
            , sum(case when bank_box_idx is not null then 1 end) over (order by height, bank_box_idx) as bb_rank
            , sum(case when oracle_price is not null then 1 end) over (order by height, bank_box_idx) as op_rank
            from combined
        ) sq
    )
    select nhs.height
		, nhs.timestamp / 1000 as timestamp
        , wrk.oracle_price
        , htx.reserves
        , htx.circ_sigusd
        , htx.circ_sigrsv
        , htc.cum_usd_erg_in - htc.cum_usd_erg_out as net_sc_erg
        , htc.cum_rsv_erg_in - htc.cum_rsv_erg_out as net_rc_erg
    from fill_blanks wrk
    left join sig.history_transactions htx on htx.bank_box_idx = wrk.bank_box_idx
    left join sig.history_transactions_cumulative htc on htc.bank_box_idx = wrk.bank_box_idx
    join node_headers nhs on nhs.height = wrk.height
    where nhs.main_chain
        and wrk.bank_box_idx is not null
    order by 1 desc;

create unique index on sig.series_history_mv(height);
refresh materialized view sig.series_history_mv;
	

/*------------------------ SigmaUSD ERG/RSV daily OHLC series ------------------------ */

create materialized view sig.sigrsv_ohlc_d_mv as
	select date
		, max(open) as o
		, max(rsv_price) as h
		, min(rsv_price) as l
		, max(close) as c
	from (
		select to_timestamp(tx.timestamp / 1000)::date as date
			, hr.rsv_price
			, first_value(hr.rsv_price) over (partition by to_timestamp(tx.timestamp / 1000)::date order by tx.timestamp) as open
			, first_value(hr.rsv_price) over (partition by to_timestamp(tx.timestamp / 1000)::date order by tx.timestamp desc) as close
		from sig.history_ratios hr
		join node_transactions tx on tx.inclusion_height = hr.height
        where tx.main_chain
	) sq
	group by 1
	order by 1
	with no data;
create unique index on sig.sigrsv_ohlc_d_mv(date);
refresh materialized view sig.sigrsv_ohlc_d_mv;


-------------------------------------------------------------------------------
-- Sync
-------------------------------------------------------------------------------
create table sig.sync_status (
	last_sync_height integer
);
insert into sig.sync_status (last_sync_height) values (0);


-- drop procedure if exists sig.sync;
create procedure sig.sync(in _height integer) as
	$$
	
	call sig.update_bank_boxes();
	call sig.update_history();

    refresh materialized view concurrently sig.series_history_mv;
    refresh materialized view concurrently sig.sigrsv_ohlc_d_mv;
	
	-- Sync Status
	update sig.sync_status
	set last_sync_height = _height;

	$$ language sql;
