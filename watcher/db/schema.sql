drop schema if exists ew cascade;
create schema ew;
create table ew.meta (
	singleton int primary key default 1,
	version integer not null,
	check(singleton = 1)
);

drop schema if exists core cascade;
create schema core;

create table core.headers (
	height int,
	id text,
	parent_id text,
	timestamp bigint
);

alter table core.headers add primary key (height);
alter table core.headers alter column id set not null;
alter table core.headers alter column parent_id set not null;
alter table core.headers alter column timestamp set not null;
alter table core.headers add constraint headers_unique_id unique(id);
alter table core.headers add constraint headers_unique_parent_id unique(parent_id);
-- alter table core.headers add constraint headers_self_fk foreign key (parent_id) references core.headers(id);

-- create table ew.outputs (
-- 	id text,
-- 	tx_id text,
-- 	header_id text,
-- 	creation_height int,
-- 	value bigint,
-- );

-- create table ew.inputs (
-- 	id text,
-- 	tx_id text,
-- 	header_id text,
-- )

-- create table ew.tokens (
-- 	box_id text,
-- 	token_id text,
-- 	header_id text,
-- 	value numeric,
-- )

-- alter table ew.tokens add primary key (box_id, token_id, header_id)

-- drop schema if exists boxes cascade;

-- -- Changes in ERG balances
-- create table bal.erg (
-- 	address text,
-- 	height int,
-- 	change bigint
-- );

-- alter table bal.erg add primary key (address, height);
-- alter table

-- -- Changes in token balances
-- create table bal.tokens (
-- 	address text not null,
-- 	token_id text,
-- 	height int,
-- 	value numeric, 
-- )

