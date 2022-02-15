-------------------------------------------------------------------------------
-- Migrations
-------------------------------------------------------------------------------
create schema ew;
create table ew.revision (
	singleton int primary key default 1,
	version integer not null,
	constraints_set boolean default false,
	check(singleton = 1)
);
insert into ew.revision (version) values (1);


-------------------------------------------------------------------------------
-- Core
------------------------------------------------------------------------------- 
create schema core;

create table core.headers (
	height int,
	id text,
	parent_id text,
	timestamp bigint
);

create table core.transactions (
	id text,
	header_id text,
	height integer,
	index integer
);

create table core.outputs (
	box_id text,
	tx_id text,
	header_id text,
	creation_height int,
	address text,
	-- settlement_height int,
	index int,
	value bigint
-- 	additional_registers json
);

create table core.inputs (
	box_id text,
	tx_id text,
	header_id text,
	index int
);

create table core.data_inputs (
	box_id text,
	tx_id text,
	header_id text,
	index int
);

create table core.box_registers (
	id smallint, -- [4,9]
	box_id text,
	value_type text,
	serialized_value text,
	rendered_value text
);

create table core.tokens (
	id text,
	box_id text,
	emission_amount bigint,
	name text,
	description text,
	decimals integer,
	standard text
);

create table core.box_assets (
	box_id text,
	token_id text,
	amount bigint
);
