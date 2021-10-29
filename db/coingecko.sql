/*
	ERG market price retrieved from Coingecko.
*/

-- drop schema cgo cascade;
create schema cgo;


create table cgo.price_at_first_of_day_block(
	timestamp bigint primary key,
	usd numeric not null
);


-- Helper function giving timestamp for all non-processed first-of-day blocks
create function cgo.get_new_first_of_day_blocks() returns bigint[]
	as $$
		with last_processed_day as (
			select 0 as timestamp -- ensure at least one row when starting from scratch
			union
			select timestamp
			from cgo.price_at_first_of_day_block
			order by 1 desc
			limit 1
		), first_of_day_blocks as (
			select extract(year from to_timestamp(nhs.timestamp / 1000)) as y
				, extract(month from to_timestamp(nhs.timestamp / 1000)) as m
				, extract(day from to_timestamp(nhs.timestamp / 1000)) as d
				, min(nhs.timestamp) as timestamp
			from node_headers nhs, last_processed_day lpd
			where main_chain
				and nhs.timestamp >= lpd.timestamp
			group by 1, 2, 3
		)
		select array_agg(fdb.timestamp)
		from first_of_day_blocks fdb
		-- Keep new blocks only
		left join cgo.price_at_first_of_day_block prc
			on prc.timestamp = fdb.timestamp
		where prc.timestamp is null
		order by 1;
	$$
	language sql;