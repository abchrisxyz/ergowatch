/*
	dis - ERG distribution metrics
	------------------------------
	
	Tracks the following:
		- unspent boxes (not quite related to distribution per se, but useful byproduct)
		- supply held in top x addresses
		- number of addresses with balance > x
	
	Evaluated daily, at every first-of-day block (UTC time).
	
	Attempting to distinguish CEX and dapp addresses from community ones.
	If/when additional CEX and/or dapp addresses are know, metrics
	would have to be reevaluated from were said address became active.
*/
-- drop schema if exists dis cascade;
create schema dis;

create table dis.cex_addresses (
	address text primary key,
	cex text not null
);

insert into dis.cex_addresses (address, cex) values
	('9fowPvQ2GXdmhD2bN54EL9dRnio3kBQGyrD3fkbHwuTXD6z1wBU', 'Coinex'),
	('9fPiW45mZwoTxSwTLLXaZcdekqi72emebENmScyTGsjryzrntUe', 'Coinex'),
	('9hU5VUSUAmhEsTehBKDGFaFQSJx574UPoCquKBq59Ushv5XYgAu', 'Kucoin'),
	('9iKFBBrryPhBYVGDKHuZQW7SuLfuTdUJtTPzecbQ5pQQzD4VykC', 'Gate.io');


create table dis.dapp_addresses (
	address text primary key,
	dapp text not null
);

insert into dis.dapp_addresses (dapp, address) values
	('SigmaUSD v2', 'MUbV38YgqHy7XbsoXWF5z7EZm524Ybdwe5p9WDrbhruZRtehkRPT92imXer2eTkjwPDfboa1pR3zb3deVKVq3H7Xt98qcTqLuSBSbHb7izzo5jphEpcnqyKJ2xhmpNPVvmtbdJNdvdopPrHHDBbAGGeW7XYTQwEeoRfosXzcDtiGgw97b2aqjTsNFmZk7khBEQywjYfmoDc9nUCJMZ3vbSspnYo3LarLe55mh2Np8MNJqUN9APA6XkhZCrTTDRZb1B4krgFY1sVMswg2ceqguZRvC9pqt3tUUxmSnB24N6dowfVJKhLXwHPbrkHViBv1AKAJTmEaQW2DN1fRmD9ypXxZk8GXmYtxTtrj3BiunQ4qzUCu1eGzxSREjpkFSi2ATLSSDqUwxtRz639sHM6Lav4axoJNPCHbY8pvuBKUxgnGRex8LEGM8DeEJwaJCaoy8dBw9Lz49nq5mSsXLeoC4xpTUmp47Bh7GAZtwkaNreCu74m9rcZ8Di4w1cmdsiK1NWuDh9pJ2Bv7u3EfcurHFVqCkT3P86JUbKnXeNxCypfrWsFuYNKYqmjsix82g9vWcGMmAcu5nagxD4iET86iE2tMMfZZ5vqZNvntQswJyQqv2Wc6MTh4jQx1q2qJZCQe4QdEK63meTGbZNNKMctHQbp3gRkZYNrBtxQyVtNLR8xEY8zGp85GeQKbb37vqLXxRpGiigAdMe3XZA4hhYPmAAU5hpSMYaRAjtvvMT3bNiHRACGrfjvSsEG9G2zY5in2YWz5X9zXQLGTYRsQ4uNFkYoQRCBdjNxGv6R58Xq74zCgt19TxYZ87gPWxkXpWwTaHogG1eps8WXt8QzwJ9rVx6Vu9a5GjtcGsQxHovWmYixgBU8X9fPNJ9UQhYyAWbjtRSuVBtDAmoV1gCBEPwnYVP5GCGhCocbwoYhZkZjFZy6ws4uxVLid3FxuvhWvQrVEDYp7WRvGXbNdCbcSXnbeTrPMey1WPaXX');


-- Number of unspent boxes
create table dis.unspent_boxes (
	timestamp bigint primary key,
	boxes bigint not null
);


-- Total ERG in top x addresses - excluding coinbase, treasury, dapps and cexs
create table dis.top_addresses_supply (
	timestamp bigint primary key,
	top10 integer, 
	top100 integer,
	top1k integer,
	top10k integer,
	cexs integer,
	dapps integer
);



-- Number of addresses with balance of at least x ERG
create table dis.address_counts_by_minimal_balance (
	timestamp bigint primary key,
	total bigint not null,
	m_0_001 bigint not null,
	m_0_01 bigint not null,
	m_0_1 bigint not null,
	m_1 bigint not null,
	m_10 bigint not null,
	m_100 bigint not null,
	m_1k bigint not null,
	m_10k bigint not null,
	m_100k bigint not null,
	m_1m bigint not null
);


-----------------------------------------------------------------------
-- Snapshots - work tables storing a single snapshot in time

-- Snapshot of unspent boxes
create table dis.unspent_boxes_snapshot (
	box_id text primary key
);

-- Snapshot of address balances
create table dis.address_balances_snapshot (
	address text primary key,
	value bigint
);
create index on dis.address_balances_snapshot (value);
-----------------------------------------------------------------------


-----------------------------------------------------------------------
-- Snapshot update procedures
create procedure dis.update_unspent_boxes_snapshot(_height integer) as
	$$
	truncate dis.unspent_boxes_snapshot;
	
	with inputs as (
		select nis.box_id
		from node_inputs nis
		join node_headers nhs on nhs.id = nis.header_id
		where nhs.main_chain and nis.main_chain
			and nhs.height <= _height
	)
	insert into dis.unspent_boxes_snapshot (box_id)
	select nos.box_id
	from node_outputs nos
	join node_headers nhs on nhs.id = nos.header_id
	left join inputs nis on nis.box_id = nos.box_id
	where nhs.main_chain and nos.main_chain
		and nis.box_id is null
		-- exclude coinbase
		and nos.address <> '2Z4YBkDsDvQj8BX7xiySFewjitqp2ge9c99jfes2whbtKitZTxdBYqbrVZUvZvKv6aqn9by4kp3LE1c26LCyosFnVnm6b6U1JYvWpYmL2ZnixJbXLjWAWuBThV1D6dLpqZJYQHYDznJCk49g5TUiS4q8khpag2aNmHwREV7JSsypHdHLgJT7MGaw51aJfNubyzSKxZ4AJXFS27EfXwyCLzW1K6GVqwkJtCoPvrcLqmqwacAWJPkmh78nke9H4oT88XmSbRt2n9aWZjosiZCafZ4osUDxmZcc5QVEeTWn8drSraY3eFKe8Mu9MSCcVU'
		and nhs.height <= _height;
	
	$$
	language sql;
	

create procedure dis.update_address_balances_snapshot() as
	$$
	truncate dis.address_balances_snapshot;
	
	insert into dis.address_balances_snapshot (address, value)
		select nos.address,
			sum(nos.value) as value
		from dis.unspent_boxes_snapshot ubs
		join node_outputs nos on nos.box_id = ubs.box_id
		group by 1;
	$$
	language sql;
-----------------------------------------------------------------------


-- Helper function to generate supply held by top x addresses from snapshots
create function dis.get_top_addresses_supply()
	returns table (
		top10 integer,
		top100 integer,
		top1k integer,
		top10k integer,
		cexs integer,
		dapps integer
	) as $$
		with address_balances as (
			select nos.address
				, sum(nos.value) / 10^9 as erg
			from dis.unspent_boxes_snapshot ubs
			join node_outputs nos on nos.box_id = ubs.box_id
			-- exclude treasury
			where nos.address <> '4L1ktFSzm3SH1UioDuUf5hyaraHird4D2dEACwQ1qHGjSKtA6KaNvSzRCZXZGf9jkfNAEC1SrYaZmCuvb2BKiXk5zW9xuvrXFT7FdNe2KqbymiZvo5UQLAm5jQY8ZBRhTZ4AFtZa1UF5nd4aofwPiL7YkJuyiL5hDHMZL1ZnyL746tHmRYMjAhCgE7d698dRhkdSeVy'
			group by 1
		), ranked_addresses as (
				select row_number() over (order by erg desc) as value_rank
					, sum(erg) over (order by erg desc rows between unbounded preceding and current row) as erg
				from address_balances bal
				left join dis.cex_addresses cex on cex.address = bal.address
				left join dis.dapp_addresses dap on dap.address = bal.address
				where cex.address is null
					and dap.address is null
				order by erg desc
		), cexs as (
			select sum(erg) as erg
			from address_balances bal
			join dis.cex_addresses cex on cex.address = bal.address
		), dapps as (
			select sum(erg) as erg
			from address_balances bal
			join dis.dapp_addresses dap on dap.address = bal.address
		)
		select 
			(select erg from ranked_addresses where value_rank = 10) as t10
			,(select erg from ranked_addresses where value_rank = 100) as t100
			,(select erg from ranked_addresses where value_rank = 1000) as t1k
			,(select erg from ranked_addresses where value_rank = 10000) as t10k
			, cex.erg as cexs
			, dap.erg as dapps
		from cexs cex, dapps dap;
	$$
	language sql;
	
	
-- Helper function giving all non-processed first-of-day blocks
create function dis.get_new_first_of_day_blocks() returns integer[]
	as $$
		with last_processed_day as (
			select 0 as timestamp -- ensure at least one row when starting from scratch
			union
			select timestamp
			from dis.address_counts_by_minimal_balance
			order by 1 desc
			limit 1
		), first_of_day_blocks as (
			select extract(year from to_timestamp(nhs.timestamp / 1000)) as y
				, extract(month from to_timestamp(nhs.timestamp / 1000)) as m
				, extract(day from to_timestamp(nhs.timestamp / 1000)) as d
				, min(nhs.height) as height
				, min(nhs.timestamp) as timestamp
			from node_headers nhs, last_processed_day lpd
			where main_chain
				and nhs.timestamp >= lpd.timestamp
			group by 1, 2, 3
		)
		select array_agg(fdb.height)
		from first_of_day_blocks fdb
		-- Keep new blocks only
		left join dis.address_counts_by_minimal_balance acs
			on acs.timestamp = fdb.timestamp
		where acs.timestamp is null
		order by 1;
	$$
	language sql;


/*
	Main sync procedure.
*/
-- drop procedure if exists dis.sync(); 
create procedure dis.sync(in _bootstrapping boolean default false) as
	$$
	declare _height integer;
	declare _timestamp bigint;
	begin
	-- Loop over every first-of-day block
 	for _height in select unnest(dis.get_new_first_of_day_blocks()) loop
		
		select timestamp from node_headers where main_chain and height = _height into _timestamp;
		
		raise notice 'Processing height % - %', _height, to_timestamp(_timestamp / 1000);
		
		-- 1. Update snapshots
		call dis.update_unspent_boxes_snapshot(_height);
		call dis.update_address_balances_snapshot();
		
		-- 2. Update number of unspent boxes
		insert into dis.unspent_boxes(timestamp, boxes)
		select _timestamp, count(*)
		from dis.unspent_boxes_snapshot;

		-- 2. Update top x supply
		insert into dis.top_addresses_supply(timestamp, top10, top100, top1k, top10k, cexs, dapps)
		select _timestamp
			, top10
			, top100
			, top1k
			, top10k
			, cexs
			, dapps
		from dis.get_top_addresses_supply();
		
		-- 3. Update address counts by balance
		insert into dis.address_counts_by_minimal_balance (
			timestamp,
			total,
			m_0_001,
			m_0_01,
			m_0_1,
			m_1,
			m_10,
			m_100,
			m_1k,
			m_10k,
			m_100k,
			m_1m
		)
		select _timestamp
			, count(*) as total
			, count(*) filter (where value >= 0.001 * 10^9)
			, count(*) filter (where value >= 0.01 * 10^9)
			, count(*) filter (where value >= 0.1 * 10^9)
			, count(*) filter (where value >= 1 * 10^9)
			, count(*) filter (where value >= 10 * 10^9)
			, count(*) filter (where value >= 100 * 10^9)
			, count(*) filter (where value >= 1000 * 10^9)
			, count(*) filter (where value >= 10000 * 10^9)
			, count(*) filter (where value >= 100000 * 10^9)
			, count(*) filter (where value >= 1000000 * 10^9)
		from dis.address_balances_snapshot bal
		left join dis.cex_addresses cex on cex.address = bal.address
		left join dis.dapp_addresses dap on dap.address = bal.address
		where cex.address is null
			and dap.address is null
			-- exclude treasury
			and bal.address <> '4L1ktFSzm3SH1UioDuUf5hyaraHird4D2dEACwQ1qHGjSKtA6KaNvSzRCZXZGf9jkfNAEC1SrYaZmCuvb2BKiXk5zW9xuvrXFT7FdNe2KqbymiZvo5UQLAm5jQY8ZBRhTZ4AFtZa1UF5nd4aofwPiL7YkJuyiL5hDHMZL1ZnyL746tHmRYMjAhCgE7d698dRhkdSeVy';
		
		-- When starting from scratch or after a long gap, commit progress.
		if _bootstrapping then commit; end if;
		
	end loop;
	
	-- 4. Refresh materialized views
	-- TODO
			
 	end;
 	$$
 	language plpgsql;
