/*
	age - Mean supply age over time (at each block).
*/
-- drop schema if exists age cascade;
create schema age;


-- Mean ERG age and transferred value for each block
create table age.block_stats(
	height integer primary key,
	circulating_supply bigint not null,
	transferred_value bigint not null,
	mean_age_ms bigint not null -- milliseconds
);


-- Initialise block stats
insert into age.block_stats (height, circulating_supply, transferred_value, mean_age_ms)
values (1, 75 * 10^9, 0, 0);


