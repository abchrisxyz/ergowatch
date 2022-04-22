-------------------------------------------------------------------------------
-- Migrations
-------------------------------------------------------------------------------
create schema ew;
create table ew.revision (
	singleton int primary key default 1,
	version integer not null,
	check(singleton = 1)
);
insert into ew.revision (version) values (5);


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
-- CEXs - Complex topic, they get their own schema :)
------------------------------------------------------------------------------- 
create schema cex;

create table cex.cexs (
	id integer,
	name text
);

insert into cex.cexs (id, name) values
	(1, 'Coinex'),
	(2, 'Gate.io'),
	(3, 'KuCoin'),
	(4, 'ProBit')
;

create type cex.t_address_type as enum ('main', 'deposit');

create table cex.addresses (
	address text,
	cex_id integer,
	type cex.t_address_type
);

-- Store new deposit addresses as they get found.
-- Will be cleared periodically.
-- Spot height is to allow for easier rollbacks.
create table cex.new_deposit_addresses (
	address text,
	cex_id integer,
	spot_height integer
);

/*
	Track CEX deposit addresses processing.

	When a new address is found in a block, it is added to
	cex.new_deposit_address and included in this table together with
	it's invalidation height (the earliest height of deposit txs),
	and marked as 'pending' processing.

	When an unprocessed block is rolled back, its deposit addresses
	are removed from cex.new_deposit_addresses and it is itself removed
	from this table.

	When an already processed block is rolled back, its deposit
	addresses are removed from the main cex.addresses and its status is
	changed from 'processed' to 'pending_rollback'.

	When a repair event is triggered, it'll pick up at the lowest
	invalidation height of all 'pending' and 'pending_rollback' blocks.
	This ensures that the repair event overwrites any changes from a
	previous repair event that included blocks not part of the main
	chain anymore.
	
	During a repair event, 'pending' and 'pending_rollback' blocks
	are set to 'processing' and 'processing_rollback' respectively.
	When a repair event is completed, the status of affected blocks
	is changed to 'processed' or 'processed_rollback'.

	Blocks with status 'processed_rollback' are not deleted in order to
	assess how often such events occur.
 */
create type cex.t_block_status as enum (
	'pending',
	'pending_rollback',
	'processing',
	'processing_rollback',
	'processed',
	'processed_rollback'
);
create table cex.block_processing_log (
	header_id text,
	height integer,
	invalidation_height integer,
	status cex.t_block_status not null
);

-- Known main addresses
insert into cex.addresses (cex_id, type, address) values
	-- Coinex
	(1, 'main', '9fowPvQ2GXdmhD2bN54EL9dRnio3kBQGyrD3fkbHwuTXD6z1wBU'),
	(1, 'main', '9fPiW45mZwoTxSwTLLXaZcdekqi72emebENmScyTGsjryzrntUe'),
			
	-- Gate - confirmed
	(2, 'main', '9iKFBBrryPhBYVGDKHuZQW7SuLfuTdUJtTPzecbQ5pQQzD4VykC'),
	-- Gate - unconfirmed
	(2, 'main', '9enQZco9hPuqaHvR7EpPRWvYbkDYoWu3NK7pQk8VFwgVnv5taQE'),
	(2, 'main', '9i7134eY3zUotQyS8nBeZDJ3SWbTPn117nCJYi977FBn9AaxhZY'),
	(2, 'main', '9gmb745thQTyoGGWxSr9hNmvipivgVbQGA6EJnBucs3nwi9yqoc'),
	(2, 'main', '9fJzuyVaRLM9Q3RZVzkau1GJVP9TDiW8GRL5p25VZ8VNXurDpaw'),
	(2, 'main', '9i1ETULiCnGMtppDAvrcYujhxX18km3ge9ZEDMnZPN6LFQbttRF'),
	(2, 'main', '9gck4LwHJK3XV2wXdYdN5S9Fe4RcFrkaqs4WU5aeiKuodJyW7qq'),
	(2, 'main', '9gv4qw7RtQyt3khtnQNxp7r7yuUazWWyTGfo7duqGj9hMtZxKP1'),
	-- created 10/2019, but listing in 10/2020
	(2, 'main', '9exS2B892HTiDkqhcWnj1nzsbYmVn7ameVb1d2jagUWTqaLxfTX'),

	-- KuCoin
	(3, 'main', '9hU5VUSUAmhEsTehBKDGFaFQSJx574UPoCquKBq59Ushv5XYgAu'),
	(3, 'main', '9i8Mci4ufn8iBQhzohh4V3XM3PjiJbxuDG1hctouwV4fjW5vBi3'),
	(3, 'main', '9guZaxPoe4jecHi6ZxtMotKUL4AzpomFf3xqXsFSuTyZoLbmUBr'),
	(3, 'main', '9iNt6wfxSc3DSaBVp22E7g993dwKUCvbGdHoEjxF8SRqj35oXvT'),
	
	-- ProBit https://discord.com/channels/668903786361651200/896711052736233482/964541753162096680
	(4, 'main', '9eg2Rz3tGogzLaVZhG1ycPj1dJtN4Jn8ySa2mnVLJyVJryb13QB');


-------------------------------------------------------------------------------
-- Metrics
------------------------------------------------------------------------------- 
create schema mtr;

-- Supply on main and deposit addresses by cex.
-- Records new values only (i.e. when main or deposit supply has changed)
create table mtr.cex_supply_details (
	height int,
	cex_id integer,
	main bigint,
	deposit bigint
);

-- Supply across all cex's, at each height.
create table mtr.cex_supply (
	height int,
	total bigint, -- main + deposit
	deposit bigint
);

-- UTxO counts
create table mtr.utxos (
	height int,
	value bigint
);
