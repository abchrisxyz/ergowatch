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
insert into ew.revision (major, minor) values (1, 11);


-------------------------------------------------------------------------------
-- Core
------------------------------------------------------------------------------- 
create schema core;

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
	address text,
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
	text_id text, -- used for easier api access
	name text
);

insert into cex.cexs (id, name, text_id) values
	(1, 'Coinex', 'coinex'),
	(2, 'Gate.io', 'gate'),
	(3, 'KuCoin', 'kucoin'),
	(4, 'ProBit', 'probit')
;

create type cex.t_address_type as enum ('main', 'deposit');

create table cex.addresses (
	address text,
	cex_id integer,
	type cex.t_address_type,
	spot_height int
);

/*
	List of addresses to be ignored.
	Mostly addresses sending to CEX main address but active
	before the listing date.

	An alternative would be to add a listing_height column to
	cex.cexs and exclude deposit candidates active before that
	height. Keeping it simple and manual for now as number of
	addresses to ignore is small.
 */
create table cex.addresses_ignored (
	address text
);

/* 
	The PK of cex.addresses is the address. An address can therefore
	be linked to a single CEX only.
	However, we can't exclude the possibility of running into addresses
	linked to more than one CEX. When an known address is linked to
	a second CEX, it will get removed from cex.addresses and stored
	here for reference.
 */
create table cex.addresses_conflicts (
	-- Same columns as cex.address
	address text,
	first_cex_id integer,
	type cex.t_address_type,
	spot_height integer,
	-- Then some info on when the conflict occurred
	conflict_spot_height integer
);

/*
	Track CEX deposit addresses processing by block.
	
	Logs the processing status of each block as well as their invalidation
	height (the earliest height of deposit txs).
	
	New blocks are added with status 'pending', indicating they haven't
	been processed in a repait event yet.

	When an unprocessed block is rolled back, its deposit addresses
	are removed from cex.addresses and it is itself removed from this table.

	When an already processed block is rolled back, its deposit
	addresses are removed from the main cex.addresses and its status is
	changed from 'processed' to 'pending_rollback'.

	When a repair event is triggered, it'll pick up at the lowest
	invalidation height of all 'pending' and 'pending_rollback' blocks.
	This ensures that the repair event overwrites any changes from a
	previous repair event that included blocks not part of the main
	chain anymore.
	
	When a repair event is completed, the status of affected blocks
	is changed to 'processed' or 'processed_rollback'.

	Blocks with status 'processed_rollback' are not deleted in order to
	assess how often such events occur.
 */
create type cex.t_block_status as enum (
	'pending',
	'pending_rollback',
	'processed',
	'processed_rollback'
);
create table cex.block_processing_log (
	header_id text,
	height integer,
	invalidation_height integer,
	status cex.t_block_status
);

-- Supply in main and deposit addresses by cex.
-- Records new values only (i.e. when main or deposit supply has changed)
create table cex.supply (
	height int,
	cex_id integer,
	main bigint,
	deposit bigint
);


-- Known main addresses
insert into cex.addresses (cex_id, type, address) values
	-- Coinex
	(1, 'main', '9fowPvQ2GXdmhD2bN54EL9dRnio3kBQGyrD3fkbHwuTXD6z1wBU'),
	(1, 'main', '9fPiW45mZwoTxSwTLLXaZcdekqi72emebENmScyTGsjryzrntUe'),
			
	-- Gate - confirmed
	(2, 'main', '9iKFBBrryPhBYVGDKHuZQW7SuLfuTdUJtTPzecbQ5pQQzD4VykC'),
	-- Gate - unconfirmed
	--
	-- Has had up to 900k, sends only to 9iKFBB, received from 2800+
	-- addresses all created in/after 10/2020.
	(2, 'main', '9gQYrh6yubA4z55u4TtsacKnaEteBEdnY4W2r5BLcFZXcQoQDcq'),
	--
	-- Sends only to 9iKFBB, received from 219 addresses all created
	-- in/after 01/2021.
	-- Receives from 9i7134 (see below).
	(2, 'main', '9enQZco9hPuqaHvR7EpPRWvYbkDYoWu3NK7pQk8VFwgVnv5taQE'),
	--
	-- Addresses below used to be thought of as Gate addresses because
	-- of the large volume passing through them and reaching 9enQZc.
	-- 1.5M+ ERG tracing back to OG mining addresses ending up in 9exS2B,
	-- then moving to 9gv4qw7 through to 9i7134 and finally to 9enQZc.
	-- 9i7134 sends to 200+ addresses, nothing to 9iKFBB, receives from
	-- 1880 addresses - some created in 10/2019, so 9i7134 likely not a
	-- Gate address and therefore excluding upstream ones as well.
	-- (2, 'main', '9i7134eY3zUotQyS8nBeZDJ3SWbTPn117nCJYi977FBn9AaxhZY'),
	-- (2, 'main', '9gmb745thQTyoGGWxSr9hNmvipivgVbQGA6EJnBucs3nwi9yqoc'),
	-- (2, 'main', '9fJzuyVaRLM9Q3RZVzkau1GJVP9TDiW8GRL5p25VZ8VNXurDpaw'),
	-- (2, 'main', '9i1ETULiCnGMtppDAvrcYujhxX18km3ge9ZEDMnZPN6LFQbttRF'),
	-- (2, 'main', '9gck4LwHJK3XV2wXdYdN5S9Fe4RcFrkaqs4WU5aeiKuodJyW7qq'),
	-- (2, 'main', '9gv4qw7RtQyt3khtnQNxp7r7yuUazWWyTGfo7duqGj9hMtZxKP1'),
	-- created 10/2019, but listing in 10/2020
	-- (2, 'main', '9exS2B892HTiDkqhcWnj1nzsbYmVn7ameVb1d2jagUWTqaLxfTX'),

	-- KuCoin
	(3, 'main', '9hU5VUSUAmhEsTehBKDGFaFQSJx574UPoCquKBq59Ushv5XYgAu'),
	(3, 'main', '9i8Mci4ufn8iBQhzohh4V3XM3PjiJbxuDG1hctouwV4fjW5vBi3'),
	(3, 'main', '9guZaxPoe4jecHi6ZxtMotKUL4AzpomFf3xqXsFSuTyZoLbmUBr'),
	(3, 'main', '9iNt6wfxSc3DSaBVp22E7g993dwKUCvbGdHoEjxF8SRqj35oXvT'),
	
	-- ProBit https://discord.com/channels/668903786361651200/896711052736233482/964541753162096680
	(4, 'main', '9eg2Rz3tGogzLaVZhG1ycPj1dJtN4Jn8ySa2mnVLJyVJryb13QB');

insert into cex.addresses_ignored (address) values
	-- Flagged as Gate deposit address.
	-- Active since July 2019.
	-- Received 2.6M+ direct from treasury.
	-- Most of it goes to:
	--    - 9gNYeyfRFUipiWZ3JR1ayDMoeh28E6J7aDQosb7yrzsuGSDqzCC
	--    - 9fdVtQVggW7a2EBE6CPKXjvtBzN8WCHcMuJd2zgzx8KRqRuwJVr
	-- Only 1 tx to 9iKFBB, 50k on 1 Oct 2020
	-- https://explorer.ergoplatform.com/en/transactions/afe34ee3128ce9c4838bc64c0530322db1b3aa3c48400ac50ede3b68ad08ddd2
	('9hxFS2RkmL5Fv5DRZGwZCbsbjTU1R75Luc2t5hkUcR1x3jWzre4'),

	-- Flagged as Gate deposit address.
	-- Active since March 2020.
	-- Received 1.5M+ from 9hxFS2 (see above)
	-- Only 1 tx to 9iKFBB: 50k on 5 Oct 2020
	-- https://explorer.ergoplatform.com/en/transactions/5e202e5e37631701db2cb0ddc839601b2da74ce7f6e826bc9244f1ada5dba92c
	('9gNYeyfRFUipiWZ3JR1ayDMoeh28E6J7aDQosb7yrzsuGSDqzCC'),

	-- Flagged as Gate deposit address.
	-- First active May 2020.
	-- Involved in 50k tx to 9iKFBB on 5 Oct 2020 (see above).
	-- Received 2 tx from 9iKFBB:
	-- 898 ERG on 30 March 2021 - https://explorer.ergoplatform.com/en/transactions/c2d592cc688ec8d8ffa7ea22e054aca31b39578ed004fcd4cbcc11783e4739db
	-- 698 ERG on 12 April 2021 - https://explorer.ergoplatform.com/en/transactions/3dd8e7015568228336a5d16c5b690e3a5653d2d827711a9b1580e0b7db13e563
	('9i2oKu3bbHDksfiZjbhAgSAWW7iZecUS78SDaB46Fpt2DpUNe6M'),

	-- Flagged as Gate deposit address.
	-- First active June 2020.
	-- Received 100 ERG from 9fPiW4 (Coinex withdrawal) in 2 txs in June 2020:
	--    - https://explorer.ergoplatform.com/en/transactions/6e73b4e7e1e0e339ba6185fd142ac2df8409e9bebcffed7b490107633695fe88
	--    - https://explorer.ergoplatform.com/en/transactions/34193dbdde8921b74ece7cae1adf495830a52fb72477c2a610b31cb4750b45f2
	-- Received 550 ERG from 9gNYey and others in June 2020 - https://explorer.ergoplatform.com/en/transactions/811b17a00d821763aa096dc3e6225122451b068454eb1cfb16bf5c7b47fea9f5
	-- Sent 20 ERG to 9iKFBB on 27 September 2020 - https://explorer.ergoplatform.com/en/transactions/8bc2caf976e5e5f0786ee54bb886f3344e6dac1c034491766e977c4b3a828305
	-- First ever tx to 9iKFBB (!)
	('9iHCMtd2gAPoYGhWadjruygKwNKRoeQGq1xjS2Fkm5bT197YFdR');

-------------------------------------------------------------------------------
-- Metrics
------------------------------------------------------------------------------- 
create schema mtr;

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
