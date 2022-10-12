/*****************************************************************************
	This is a snapshot of the db schema that shipped with v0.5, prior to any
	migrations.	It's sole purpose is to test db migrations.
*****************************************************************************/
-------------------------------------------------------------------------------
-- Migrations
-------------------------------------------------------------------------------
create schema ew;
create table ew.revision (
	singleton int primary key default 1,
	major integer not null,
	minor integer not null,
	check(singleton = 1)
);
insert into ew.revision (major, minor) values (3, 0);

create table ew.constraints (
	singleton int primary key default 1,
	tier_1 boolean not null default false,
	tier_2 boolean not null default false,
	check(singleton = 1)
);
insert into ew.constraints (singleton) values (1);

create table ew.repairs (
	singleton int primary key default 1,
	-- creation timestamp - usefull for debugging
	started timestamp not null,
	check(singleton = 1)
);

-------------------------------------------------------------------------------
-- Core
------------------------------------------------------------------------------- 
create schema core;

create table core.addresses (
	id bigint,
	address text,
	spot_height int
);
-- These are needed to add new addresses, so declaring here and not in constraints.sql.
create index on core.addresses (md5(address));
alter table core.addresses add exclude using hash (address with=);

create function core.address_id(_address text) returns bigint as '
	select id
	from core.addresses
	where md5(address) = md5($1)
		and address = $1;'
    language sql
    immutable
    returns null on null input;

create table core.headers (
	height int,
	id text,
	parent_id text,
	timestamp bigint,
	difficulty bigint,
	vote1 smallint,
	vote2 smallint,
	vote3 smallint
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
	address_id bigint,
	index int,
	value bigint,
	size integer -- box size in bytes
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

create table core.system_parameters (
	height integer,
	storage_fee integer,        -- 1. Storage fee nanoErg/byte
	min_box_value integer,      -- 2. Minimum box value in nanoErg
	max_block_size integer,     -- 3. Maximum block size
	max_cost integer,           -- 4. Maximum computational cost of a block
	token_access_cost integer,  -- 5. Token access cost
	tx_input_cost integer,      -- 6. Cost per tx input
	tx_data_input_cost integer, -- 7. Cost per tx data-input
	tx_output_cost integer,     -- 8. Cost per tx output
	block_version integer       -- 123. Block version
);


-- Placeholder for unhandled extension fields.
-- Just storing whatever k,v show up for later processing.
create table core.unhandled_extension_fields (
	height integer,
	key text,
	value text
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

-- U can't touch this too-do-do-do
create table bal._log (
	singleton int primary key default 1,
	constraints_set bool not null default FALSE,
	bootstrapped bool not null default FALSE,
	check(singleton = 1)
);
insert into bal._log(singleton) values (1);

-- Running ERG balances
create table bal.erg (
	address_id bigint,
	value bigint
);

-- Changes in ERG balances
create table bal.erg_diffs (
	address_id bigint,
	height int,
	tx_id text,
	value bigint
);

-- Running token balances
create table bal.tokens (
	address_id bigint,
	token_id text,
	value bigint
);

-- Changes in token balances
create table bal.tokens_diffs (
	address_id bigint,
	token_id text,
	height int,
	tx_id text,
	value bigint
);