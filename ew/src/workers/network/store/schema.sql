create schema if not exists network;
comment on schema network is 'Network stats and properties';

create table network.parameters (
    height integer primary key,
	storage_fee integer not null,        -- 1. Storage fee nanoErg/byte
	min_box_value integer not null,      -- 2. Minimum box value in nanoErg
	max_block_size integer not null,     -- 3. Maximum block size
	max_cost integer not null,           -- 4. Maximum computational cost of a block
	token_access_cost integer not null,  -- 5. Token access cost
	tx_input_cost integer not null,      -- 6. Cost per tx input
	tx_data_input_cost integer not null, -- 7. Cost per tx data-input
	tx_output_cost integer not null,     -- 8. Cost per tx output
	block_version integer not null       -- 123. Block version
);

create table network.votes (
    height integer primary key,
	slots smallint[3] not null
);

create table network.proposals (
	epoch integer primary key,
	height integer unique not null,
	-- Proposed by
	miner_address_id bigint not null,
	-- Proposed change in slots 1/2/3
	slots smallint[3] not null,
	-- Number of yes votes for proposed change in slots 1/2/3
	tallies smallint[3] not null
);

-- Placeholder for unhandled extension fields.
-- Just storing whatever k,v show up for later processing.
create table network._unhandled_extension_fields (
	height integer,
	-- The two key bytes as one i16
	-- key / 256 to get first u8
	-- key % 256 to get second u8
	key smallint,
	-- Raw base16 encoded value
	value_base16 text
);

create table network.transactions (
    height integer primary key,
    transactions integer not null,
    user_transactions integer not null
);

create table network.mining (
	height integer primary key,
	miner_address_id bigint not null,
	difficulty numeric not null,
	difficulty_24h_mean numeric not null,
    hash_rate_24h_mean bigint not null, -- hashes per second
    block_reward bigint not null,
    tx_fees bigint not null
);

create table network.known_miners (
	address_id bigint primary key,
	label text not null,
	mining_pool text
);
insert into network.known_miners (address_id, label, mining_pool) values
	(1407062, 'Wooly Pooly', 'Wooly Pooly'),
	(2409982, 'Hero Miners', 'Hero Miners'),
	(3796992, 'SOLO Pool', 'SOLO Pool'),
	(3974222, '2miners', '2miners'),
	(4156212, 'K1 Pool', 'K1 Pool'),
	(6424182, 'JJ Pool', 'JJ Pool'),
	(6498622, 'Nano Pool', 'Nano Pool'),
	(6854082, 'DX Pool', 'DX Pool'),
	(6990442, 'Magic Pool', 'Magic Pool'),
	(7948072, '666 Pool', '666 Pool');

