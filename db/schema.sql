-------------------------------------------------------------------------------
-- Migrations and repairs
-------------------------------------------------------------------------------
create schema ew;
create table ew.revision (
	singleton int primary key default 1,
	major integer not null,
	minor integer not null,
	check(singleton = 1)
);
insert into ew.revision (major, minor) values (3, 27);

create table ew.repairs (
	singleton int primary key default 1,
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
	spot_height int,
	p2pk boolean,
	miner boolean
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
-- Address properties
------------------------------------------------------------------------------- 
create schema adr;

-- Internal usage
create table adr._log (
	singleton int primary key default 1,
	constraints_set bool not null default FALSE,
	bootstrapped bool not null default FALSE,
	check(singleton = 1)
);
insert into adr._log(singleton) values (1);

-- Running ERG balances
create table adr.erg (
	address_id bigint,
	value bigint,
	mean_age_timestamp bigint
);

-- Changes in ERG balances
create table adr.erg_diffs (
	address_id bigint,
	height int,
	tx_id text,
	value bigint
);

-- Running token balances
create table adr.tokens (
	address_id bigint,
	token_id text,
	value bigint
);

-- Changes in token balances
create table adr.tokens_diffs (
	address_id bigint,
	token_id text,
	height int,
	tx_id text,
	value bigint
);


-------------------------------------------------------------------------------
-- Block level info
------------------------------------------------------------------------------- 
create schema blk;

-- Internal usage
create table blk._log (
	singleton int primary key default 1,
	constraints_set bool not null default FALSE,
	bootstrapped bool not null default FALSE,
	check(singleton = 1)
);
insert into blk._log(singleton) values (1);

create table blk.stats (
	height int,
	circulating_supply bigint,
	emission bigint,
	reward bigint,
	tx_fees bigint,
	tx_count bigint,
	volume bigint -- erg changing addresses, excluding emission
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
	(4, 'ProBit', 'probit'),
	(5, 'TradeOgre', 'tradeogre')
;

create type cex.t_address_type as enum ('main', 'deposit');

create table cex.addresses (
	address_id bigint,
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
	address_id bigint
);

/*
	List of known main addresses to be populated manually.

	Corresponding address id's will be added to cex.addresses
	automatically once available. 
*/
create table cex.main_addresses_list (
	address text,
	cex_id integer
);

/*
	List of ignored addresses to be populated manually.

	Corresponding address id's will be added to cex.addresses_ignored
	automatically once available. 
*/
create table cex.ignored_addresses_list (
	address text
);

/* 
	The PK of cex.addresses is the address id. An address can therefore
	be linked to a single CEX only.
	However, we can't exclude the possibility of running into addresses
	linked to more than one CEX. When a known address is linked to
	a second CEX, it will get removed from cex.addresses and stored
	here for reference.
 */
create table cex.addresses_conflicts (
	-- Same columns as cex.address
	address_id bigint,
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
insert into cex.main_addresses_list (cex_id, address) values
	-- Coinex
	(1, '9fowPvQ2GXdmhD2bN54EL9dRnio3kBQGyrD3fkbHwuTXD6z1wBU'),
	(1, '9fPiW45mZwoTxSwTLLXaZcdekqi72emebENmScyTGsjryzrntUe'),
			
	-- Gate - confirmed
	(2, '9iKFBBrryPhBYVGDKHuZQW7SuLfuTdUJtTPzecbQ5pQQzD4VykC'),
	-- Gate - unconfirmed
	--
	-- Has had up to 900k, sends only to 9iKFBB, received from 2800+
	-- addresses all created in/after 10/2020.
	(2, '9gQYrh6yubA4z55u4TtsacKnaEteBEdnY4W2r5BLcFZXcQoQDcq'),
	--
	-- Sends only to 9iKFBB, received from 219 addresses all created
	-- in/after 01/2021.
	-- Receives from 9i7134 (see below).
	(2, '9enQZco9hPuqaHvR7EpPRWvYbkDYoWu3NK7pQk8VFwgVnv5taQE'),
	--
	-- Addresses below used to be thought of as Gate addresses because
	-- of the large volume passing through them and reaching 9enQZc.
	-- 1.5M+ ERG tracing back to OG mining addresses ending up in 9exS2B,
	-- then moving to 9gv4qw7 through to 9i7134 and finally to 9enQZc.
	-- 9i7134 sends to 200+ addresses, nothing to 9iKFBB, receives from
	-- 1880 addresses - some created in 10/2019, so 9i7134 likely not a
	-- Gate address and therefore excluding upstream ones as well.
	-- (2, '9i7134eY3zUotQyS8nBeZDJ3SWbTPn117nCJYi977FBn9AaxhZY'),
	-- (2, '9gmb745thQTyoGGWxSr9hNmvipivgVbQGA6EJnBucs3nwi9yqoc'),
	-- (2, '9fJzuyVaRLM9Q3RZVzkau1GJVP9TDiW8GRL5p25VZ8VNXurDpaw'),
	-- (2, '9i1ETULiCnGMtppDAvrcYujhxX18km3ge9ZEDMnZPN6LFQbttRF'),
	-- (2, '9gck4LwHJK3XV2wXdYdN5S9Fe4RcFrkaqs4WU5aeiKuodJyW7qq'),
	-- (2, '9gv4qw7RtQyt3khtnQNxp7r7yuUazWWyTGfo7duqGj9hMtZxKP1'),
	-- created 10/2019, but listing in 10/2020
	-- (2, '9exS2B892HTiDkqhcWnj1nzsbYmVn7ameVb1d2jagUWTqaLxfTX'),

	-- KuCoin
	(3, '9hU5VUSUAmhEsTehBKDGFaFQSJx574UPoCquKBq59Ushv5XYgAu'),
	(3, '9i8Mci4ufn8iBQhzohh4V3XM3PjiJbxuDG1hctouwV4fjW5vBi3'),
	(3, '9guZaxPoe4jecHi6ZxtMotKUL4AzpomFf3xqXsFSuTyZoLbmUBr'),
	(3, '9iNt6wfxSc3DSaBVp22E7g993dwKUCvbGdHoEjxF8SRqj35oXvT'),
	
	-- ProBit https://discord.com/channels/668903786361651200/896711052736233482/964541753162096680
	(4, '9eg2Rz3tGogzLaVZhG1ycPj1dJtN4Jn8ySa2mnVLJyVJryb13QB'),

	-- TradeOgre
	(5, '9fs99SejQxDjnjwrZ13YMZZ3fwMEVXFewpWWj63nMhZ6zDf2gif');
	

insert into cex.ignored_addresses_list (address) values
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
-- Coingecko
------------------------------------------------------------------------------- 
create schema cgo;

-- Raw hourly ERG/USD from Coingecko (market_chart api)
create table cgo.ergusd (
	timestamp bigint primary key not null, -- in ms, just like in core.headers
	value double precision not null
);


-------------------------------------------------------------------------------
-- Metrics
------------------------------------------------------------------------------- 
create schema mtr;

-- Internal usage
create table mtr._log (
	singleton int primary key default 1,
	ergusd_constraints_set bool not null default FALSE,
	ergusd_bootstrapped bool not null default FALSE,
	address_counts_constraints_set bool not null default FALSE,
	address_counts_bootstrapped bool not null default FALSE,
	supply_age_bootstrapped bool not null default FALSE,
	supply_age_constraints_set bool not null default FALSE,
	supply_composition_bootstrapped bool not null default FALSE,
	supply_composition_constraints_set bool not null default FALSE,
	supply_distribution_bootstrapped bool not null default FALSE,
	supply_distribution_constraints_set bool not null default FALSE,
	timestamps_constraints_set bool not null default FALSE,
	transactions_constraints_set bool not null default FALSE,
	volume_constraints_set bool not null default FALSE,
	check(singleton = 1)
);
insert into mtr._log(singleton) values (1);


-- Chain height at daily and hourly timestamps, including latest available height
create table mtr.timestamps_daily (
	height int,
	timestamp bigint
);
create table mtr.timestamps_hourly (
	height int,
	timestamp bigint
);

-- ERG/USD value used at each height.
-- Most values will be interpolated from cgo.ergusd.
-- Last couple of values will be latest available from cgo.ergusd
-- while waiting for next datapoint to be available. Such heights are
-- recorded in mtr.ergusd_provisional (see below) so they can be adjusted
-- when possible.
-- This takes care of possible Coingecko outages and ensures consistency
-- across different watcher instances.
create table mtr.ergusd (
	height int,
	value double precision
);

-- Height for which no ERG/USD could be interpolated yet and for which
-- the latest available value is used instead.
create table mtr.ergusd_provisional (
	height int
);

-- UTxO counts
create table mtr.utxos (
	height int,
	value bigint
);

-- Address counts by balance
------------------------------------------------------------------------------- 
create table mtr.address_counts_by_balance_p2pk (
	height int,
	total bigint,
	ge_0p001 bigint,
	ge_0p01 bigint,
	ge_0p1 bigint,
	ge_1 bigint,
	ge_10 bigint,
	ge_100 bigint,
	ge_1k bigint,
	ge_10k bigint,
	ge_100k bigint,
	ge_1m bigint
);
create table mtr.address_counts_by_balance_contracts (
	height int,
	total bigint,
	ge_0p001 bigint,
	ge_0p01 bigint,
	ge_0p1 bigint,
	ge_1 bigint,
	ge_10 bigint,
	ge_100 bigint,
	ge_1k bigint,
	ge_10k bigint,
	ge_100k bigint,
	ge_1m bigint
);
create table mtr.address_counts_by_balance_miners (
	height int,
	total bigint,
	ge_0p001 bigint,
	ge_0p01 bigint,
	ge_0p1 bigint,
	ge_1 bigint,
	ge_10 bigint,
	ge_100 bigint,
	ge_1k bigint,
	ge_10k bigint,
	ge_100k bigint,
	ge_1m bigint
);


-- Supply composition
------------------------------------------------------------------------------- 
-- Emitted supply by address type
-- Total will exceed actual CS because some of miner's supply is meant for
-- re-emission contract.
-- Sum of all terms = emitted supply.
-- Emitted supply >= circualting supply (because of reemissions still on
-- miner contracts)
create table mtr.supply_composition (
	height int,
	-- supply on p2pk addresses, excluding main cex addresses
	p2pks bigint,
	-- supply on main cex addresses
	cex_main bigint,
	-- supply on cex deposit addresses
	cex_deposits bigint,
	-- contracts excluding treasury
	contracts bigint,
	-- all supply on miner addresses, including destined to reemission.
	miners bigint,
	-- unlocked treasury supply (boils down to treasury balance after first 2.5 years)
	treasury bigint
);


-- Circulating supply distribution
------------------------------------------------------------------------------- 

-- Circulating supply on p2pk addresses excluding main exchange addresses
create table mtr.supply_on_top_addresses_p2pk (
	height int,
	top_1_prc bigint,
	top_1k bigint,
	top_100 bigint,
	top_10 bigint
);

-- Circulating supply on contract addresses excluding EF treasury and miners
create table mtr.supply_on_top_addresses_contracts (
	height int,
	top_1_prc bigint,
	top_1k bigint,
	top_100 bigint,
	top_10 bigint
);

-- Circulating supply on mining contracts (starting with '88dhgzEuTX')
create table mtr.supply_on_top_addresses_miners (
	height int,
	top_1_prc bigint,
	top_1k bigint,
	top_100 bigint,
	top_10 bigint
);

-- Supply across all cex's, at each height.
create table mtr.cex_supply (
	height int,
	total bigint, -- main + deposit
	deposit bigint
);

-- Mean age timestamp of supply
create table mtr.supply_age_timestamps (
	height int,
	overall bigint,    -- all supply not in (re)emission contracts
	p2pks bigint,      -- excluding main cex addresses
	cexs bigint,       -- main cex addresses
	contracts bigint,  -- excluding EF treasury
	miners bigint      -- mining contrats
);
-- Mean supply age in days
create table mtr.supply_age_days (
	height int,
	overall real,      -- all supply not in (re)emission contracts
	p2pks real,        -- excluding main cex addresses
	cexs real,         -- main cex addresses
	contracts real,    -- excluding EF treasury
	miners real        -- mining contracts
);

create table mtr.transactions(
	height int,
	daily_1d bigint,    -- txs/day over past 24h
	daily_7d bigint,    -- txs/day over past 7 days
	daily_28d bigint    -- txs/day over past 4 weeks
);

create table mtr.volume(
	height int,
	daily_1d bigint,    -- daily volume over past 24h
	daily_7d bigint,    -- daily volume over past 7 days
	daily_28d bigint    -- daily volume over past 4 weeks
);
