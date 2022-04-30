-------------------------------------------------------------------------------
-- Migrations
-------------------------------------------------------------------------------
create schema ew;
create table ew.revision (
	singleton int primary key default 1,
	version integer not null,
	check(singleton = 1)
);
insert into ew.revision (version) values (4);


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


-------------------------------------------------------------------------------
-- Unpent boxes
-------------------------------------------------------------------------------
create schema usp;

create table usp.boxes (
	box_id text
);


-------------------------------------------------------------------------------
-- Balances
------------------------------------------------------------------------------- 
create schema bal;

-- Running ERG balances
create table bal.erg (
	address text,
	value bigint
);

-- Changes in ERG balances
create table bal.erg_diffs (
	address text,
	height int,
	tx_id text,
	value bigint
);

-- Running token balances
create table bal.tokens (
	address text,
	token_id text,
	value bigint
);

-- Changes in token balances
create table bal.tokens_diffs (
	address text,
	token_id text,
	height int,
	tx_id text,
	value bigint
);


-------------------------------------------------------------------------------
-- Metrics
------------------------------------------------------------------------------- 
create schema mtr;

-- UTxO counts
create table mtr.utxos (
	height int,
	value bigint
);
