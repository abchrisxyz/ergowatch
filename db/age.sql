/*
	age - Mean ERG age over time (at each block).
	
	Definitions:
	 - h: height
	 - s(h): circulating supply at h
	 - a(h): mean supply age at h
	 - e(h): coinbase emission for block h
	 - x(h): transferred ERG in block h, excluding r(h)
	 - t(h): time between current and previous block
	
	At h = 1
	--------
	s(1) = e(1)
	a(1) = 0
	x(1) = 0
	
	At h = 2
	--------
	x(2) <= s(1)
	s(2) = s(1) + e(2)
	a(2) = [ (s(1) - x(2)) * (a(1) + t(h)) ] / s(2)
	
	At h = n
	--------
	x(n) <= s(n-1)
	s(n) = s(n-1) + e(n)
	a(n) = [ (s(n-1) - x(n)) * (a(n-1) + t(n)) ] / s(n)
*/
-- drop schema if exists age cascade;
create schema age;


-- Mean ERG age and transferred value for each block
create table age.block_stats(
	height integer primary key,
	circulating_supply bigint,
	transferred_value bigint,
	mean_age_seconds numeric,
	mean_age_heights numeric
);


-- Initialise block stats
insert into age.block_stats (height, circulating_supply, transferred_value, mean_age_seconds, mean_age_heights)
values (1, 75 * 10^9, 0, 0, 0);


/*
	Emission at given height
*/
create function age.get_emission(_height integer) returns bigint
	as $$
	select case
		when _height <= 525600 then 75
		else 75 - (((_height::integer - 525600) / 64800) + 1) * 3
		end * 10^9;
	$$ language sql;
	

create function age.get_seconds_since_previous_block(_height integer) returns bigint
	as $$
		select (timestamp - lag(timestamp) over (order by height)) / 1000
		from node_headers
		where main_chain
			and height = _height or height = _height + 1
		order by height desc limit 1;
	$$
	language sql;


/*
	Calculate transferred value within a block.
	Transferred value is nanoerg transfered to different address, excluding coinbase emissions.

	With miners mining their own transactions, the tx fees can end back into emitting address.
	See block 3355 for an example.
	Ideally those tx fees would not be counted as "transferred value". This is ignored here
	to keep things simple.
*/
create function age.get_block_transferred_value(_height integer)	returns bigint
	as $$
	with transactions as (
		select inclusion_height as height, id
		from node_transactions
		where main_chain
			and inclusion_height = _height
	), inputs as (
		select txs.id as tx_id
			, nos.address
			, sum(nos.value) as value
		from transactions txs
		join node_inputs nis on nis.tx_id = txs.id
		join node_outputs nos on nos.box_id = nis.box_id
		where nis.main_chain and nos.main_chain
			-- exclude coinbase emission txs
			and nos.address <> '2Z4YBkDsDvQj8BX7xiySFewjitqp2ge9c99jfes2whbtKitZTxdBYqbrVZUvZvKv6aqn9by4kp3LE1c26LCyosFnVnm6b6U1JYvWpYmL2ZnixJbXLjWAWuBThV1D6dLpqZJYQHYDznJCk49g5TUiS4q8khpag2aNmHwREV7JSsypHdHLgJT7MGaw51aJfNubyzSKxZ4AJXFS27EfXwyCLzW1K6GVqwkJtCoPvrcLqmqwacAWJPkmh78nke9H4oT88XmSbRt2n9aWZjosiZCafZ4osUDxmZcc5QVEeTWn8drSraY3eFKe8Mu9MSCcVU'
			-- exclude miner fees reward txs as already included in other txs
			-- see 3 txs of block 3357 for an example
			and nos.address <> '2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe'
		group by 1, 2
	), outputs as (
		select txs.id as tx_id
			, nos.address
			, sum(nos.value) as value
		from transactions txs
		join node_outputs nos on nos.tx_id = txs.id
		where nos.main_chain
		group by 1, 2
	)
	select coalesce(sum(i.value - coalesce(o.value, 0)), 0) as value
	from inputs i
	left join outputs o
		on o.address = i.address
		and o.tx_id = i.tx_id;
	$$ language sql;


/*
	Derive block stats for next height
*/
create procedure age.step_block_stats()
	as $$
	insert into age.block_stats (height, circulating_supply, transferred_value, mean_age_seconds, mean_age_heights)
		select height + 1 as height
			, circulating_supply + age.get_emission(height + 1) as circulating_supply
			, age.get_block_transferred_value(height + 1) as transferred_value
			, (
				(circulating_supply - age.get_block_transferred_value(height + 1))
				*
				(mean_age_seconds + age.get_seconds_since_previous_block(height + 1))
			) / (circulating_supply + age.get_emission(height + 1)) as mean_age_seconds
			, (
				(circulating_supply - age.get_block_transferred_value(height + 1))
				*
				(mean_age_heights + 1)
			) / (circulating_supply + age.get_emission(height + 1)) as mean_age_heights
		from age.block_stats
		order by height desc
		limit 1;
	$$
	language sql;


/*
	Sync from last processed height till last mined height.
	
	This will take a while if starting from scratch.
*/
-- drop procedure age.sync();
create procedure age.sync(in _bootstrapping boolean default false) as
	$$
	declare _height integer;
	begin
	for _height in
		select generate_series(
			-- from last processed height + 1
			(select height from age.block_stats order by 1 desc limit 1) + 1,
			-- to current height
			(select height from node_headers order by 1 desc limit 1)
		)
	loop
		
		call age.step_block_stats();
		
		-- Report progress every 1000 blocks
		if _height % 1000 = 0 then
			raise notice 'Processed age for block %', _height;
			-- Commit progress if starting from scratch or after a long gap.
			if _bootstrapping then commit; end if;
		end if;
 		
	end loop;

	end;
	$$
	language plpgsql;
