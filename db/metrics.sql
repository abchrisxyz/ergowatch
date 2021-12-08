/*
	mtr - on-chain metrics
	----------------------

	Tracks the following:
		- addresses: P2PK address counts
		- contracts: P2S(H) address counts
		- distribution: supply in P2PK addresses
		- TVL: supply in P2S(H) addresses
		- CEX's: supply in known CEX addresses
		- unspent boxes (not quite related to distribution per se, but useful byproduct)
		- supply held in top x addresses
		- number of addresses with balance > x
*/

create schema mtr;


create table mtr.cex_addresses (
	address text primary key,
	cex text not null
);


insert into mtr.cex_addresses (address, cex) values
	-- Coinex
	('9fowPvQ2GXdmhD2bN54EL9dRnio3kBQGyrD3fkbHwuTXD6z1wBU', 'coinex'),
	('9fPiW45mZwoTxSwTLLXaZcdekqi72emebENmScyTGsjryzrntUe', 'coinex'),
	-- Gate - confirmed
	('9iKFBBrryPhBYVGDKHuZQW7SuLfuTdUJtTPzecbQ5pQQzD4VykC', 'gate'),
	-- Gate - unconfirmed
	('9enQZco9hPuqaHvR7EpPRWvYbkDYoWu3NK7pQk8VFwgVnv5taQE', 'gate'),
	('9i7134eY3zUotQyS8nBeZDJ3SWbTPn117nCJYi977FBn9AaxhZY', 'gate'),
	('9gmb745thQTyoGGWxSr9hNmvipivgVbQGA6EJnBucs3nwi9yqoc', 'gate'),
	('9fJzuyVaRLM9Q3RZVzkau1GJVP9TDiW8GRL5p25VZ8VNXurDpaw', 'gate'),
	('9i1ETULiCnGMtppDAvrcYujhxX18km3ge9ZEDMnZPN6LFQbttRF', 'gate'),
	('9gck4LwHJK3XV2wXdYdN5S9Fe4RcFrkaqs4WU5aeiKuodJyW7qq', 'gate'),
	('9gv4qw7RtQyt3khtnQNxp7r7yuUazWWyTGfo7duqGj9hMtZxKP1', 'gate'),
	('9exS2B892HTiDkqhcWnj1nzsbYmVn7ameVb1d2jagUWTqaLxfTX', 'gate'), -- created 10/2019, but listing in 10/2020
	-- Kucoin
	('9hU5VUSUAmhEsTehBKDGFaFQSJx574UPoCquKBq59Ushv5XYgAu', 'kucoin'),
	('9i8Mci4ufn8iBQhzohh4V3XM3PjiJbxuDG1hctouwV4fjW5vBi3', 'kucoin'),
	('9guZaxPoe4jecHi6ZxtMotKUL4AzpomFf3xqXsFSuTyZoLbmUBr', 'kucoin'),
	('9iNt6wfxSc3DSaBVp22E7g993dwKUCvbGdHoEjxF8SRqj35oXvT', 'kucoin');


-------------------------------------------------------------------------------
-- Addresses
-------------------------------------------------------------------------------

-- Number of P2PK addresses by minimal balance.
create table mtr.address_counts_by_minimal_balance (
	timestamp bigint primary key,
	total bigint not null,
	gte_0_001 bigint not null,
	gte_0_01 bigint not null,
	gte_0_1 bigint not null,
	gte_1 bigint not null,
	gte_10 bigint not null,
	gte_100 bigint not null,
	gte_1k bigint not null,
	gte_10k bigint not null,
	gte_100k bigint not null,
	gte_1m bigint not null
);


create table mtr.address_counts_by_minimal_balance_change_summary (
	col text primary key,
	latest bigint,
	diff_1d bigint,
	diff_1w bigint,
	diff_4w bigint,
	diff_6m bigint,
	diff_1y bigint
);


-------------------------------------------------------------------------------
-- Contracts
-------------------------------------------------------------------------------

-- Number of P2S(H) addresses by minimal balance.
create table mtr.contract_counts_by_minimal_balance (
	timestamp bigint primary key,
	total bigint not null,
	gte_0_001 bigint not null,
	gte_0_01 bigint not null,
	gte_0_1 bigint not null,
	gte_1 bigint not null,
	gte_10 bigint not null,
	gte_100 bigint not null,
	gte_1k bigint not null,
	gte_10k bigint not null,
	gte_100k bigint not null,
	gte_1m bigint not null
);


create table mtr.contract_counts_by_minimal_balance_change_summary (
	col text primary key,
	latest bigint,
	diff_1d bigint,
	diff_1w bigint,
	diff_4w bigint,
	diff_6m bigint,
	diff_1y bigint
);


-------------------------------------------------------------------------------
-- Distribution - P2PK supply excluding CEX's
-------------------------------------------------------------------------------

-- Supply (nanoERG) in top x P2PK addresses - excluding cexs
create table mtr.top_addresses_supply (
	timestamp bigint primary key,
	top10 bigint,
	top100 bigint,
	top1k bigint,
	top10k bigint,
	total bigint,
	circulating_supply bigint
);


create table mtr.top_addresses_supply_change_summary (
	col text primary key,
	latest numeric,
	diff_1d numeric,
	diff_1w numeric,
	diff_4w numeric,
	diff_6m numeric,
	diff_1y numeric
);


-------------------------------------------------------------------------------
-- TVL - P2S(H) supply excluding treasury
-------------------------------------------------------------------------------

-- Supply (nanoERG) in top x P2S(H) addresses - excluding treasury
create table mtr.top_contracts_supply (
	timestamp bigint primary key,
	top10 bigint,
	top100 bigint,
	top1k bigint,
	top10k bigint,
	total bigint,
	circulating_supply bigint
);


create table mtr.top_contracts_supply_change_summary (
	col text primary key,
	latest numeric,
	diff_1d numeric,
	diff_1w numeric,
	diff_4w numeric,
	diff_6m numeric,
	diff_1y numeric
);


-------------------------------------------------------------------------------
-- CEX's
-------------------------------------------------------------------------------

-- Supply in known CEX addresses
create table mtr.cex_addresses_supply (
	timestamp bigint,
	address text,
	nano bigint,
	primary key(timestamp, address)
);


create table mtr.cexs_supply (
	timestamp bigint primary key,
	circulating_supply bigint,
	total bigint,
	coinex bigint,
	gateio bigint,
	kucoin bigint
);


create table mtr.cexs_supply_change_summary (
	col text primary key,
	latest numeric,
	diff_1d numeric,
	diff_1w numeric,
	diff_4w numeric,
	diff_6m numeric,
	diff_1y numeric
);


-------------------------------------------------------------------------------
-- UTXO's
-------------------------------------------------------------------------------

-- Number of unspent boxes
create table mtr.unspent_boxes (
	timestamp bigint primary key,
	boxes bigint not null
);


create table mtr.unspent_boxes_change_summary (
	col text primary key,
	latest bigint,
	diff_1d bigint,
	diff_1w bigint,
	diff_4w bigint,
	diff_6m bigint,
	diff_1y bigint
);


-- "Dust" list: top addresses by utxo count
create table mtr.unspent_boxes_top_addresses (
	address text primary key,
	boxes bigint not null
);


-- Latest values
create table mtr.preview (
	singleton integer primary key default 1 check(singleton = 1),
	timestamp bigint,
	total_addresses bigint,
	total_contracts bigint,
	top100_supply_fraction numeric,
	contracts_supply_fraction numeric,
	cexs_supply_fraction numeric,
	boxes bigint
);
