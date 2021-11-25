/*
	con - Continous block stats.
*/
-- drop schema if exists age cascade;
create schema con;


-- Mean ERG age and transferred value for each block
create table con.block_stats(
	height integer primary key,
	circulating_supply bigint not null,
	transferred_value bigint not null,
	mean_age_ms bigint not null, -- milliseconds
	transactions bigint not null
);


create table con.mean_age_series_daily (
	timestamp bigint primary key, -- first of day block
	mean_age_days float not null
)


create table con.aggregate_series_daily (
	timestamp bigint primary key, -- first of day block
	transferred_value bigint not null,
	transactions bigint not null
);


-- Initialise block stats
insert into con.block_stats (height, circulating_supply, transferred_value, mean_age_ms, transactions)
values (1, 75 * 10^9, 0, 0, 1);


