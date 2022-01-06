/*
	ERG market price retrieved from Coingecko.
*/

drop schema if exists cgo cascade;
create schema cgo;


create table cgo.price_at_first_of_day_block(
	timestamp bigint primary key,
	usd numeric not null,
	coingecko_ts bigint not null
);


-- Some metrics are timestamped on last of day block.
-- This is just for easy alignment of such metrics with price data.
create table cgo.price_at_last_of_day_block(
	timestamp bigint primary key,
	usd numeric not null,
	coingecko_ts bigint not null
);
