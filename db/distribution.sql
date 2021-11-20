/*
	dis - ERG distribution metrics
	------------------------------
	
	Tracks the following:
		- unspent boxes (not quite related to distribution per se, but useful byproduct)
		- supply held in top x addresses
		- number of addresses with balance > x
	
	Evaluated daily, at every first-of-day block (UTC time).
	
	Attempting to distinguish CEX and dapp addresses from community ones.
	If/when additional CEX and/or dapp addresses are know, metrics
	would have to be reevaluated from were said address became active.
*/
-- drop schema if exists dis cascade;
create schema dis;

create table dis.cex_addresses (
	address text primary key,
	cex text not null
);

insert into dis.cex_addresses (address, cex) values
	('9fowPvQ2GXdmhD2bN54EL9dRnio3kBQGyrD3fkbHwuTXD6z1wBU', 'Coinex'),
	('9fPiW45mZwoTxSwTLLXaZcdekqi72emebENmScyTGsjryzrntUe', 'Coinex'),
	('9hU5VUSUAmhEsTehBKDGFaFQSJx574UPoCquKBq59Ushv5XYgAu', 'Kucoin'),
	('9iKFBBrryPhBYVGDKHuZQW7SuLfuTdUJtTPzecbQ5pQQzD4VykC', 'Gate.io');


create table dis.dapp_addresses (
	address text primary key,
	dapp text not null
);

insert into dis.dapp_addresses (dapp, address) values
	('SigmaUSD v2', 'MUbV38YgqHy7XbsoXWF5z7EZm524Ybdwe5p9WDrbhruZRtehkRPT92imXer2eTkjwPDfboa1pR3zb3deVKVq3H7Xt98qcTqLuSBSbHb7izzo5jphEpcnqyKJ2xhmpNPVvmtbdJNdvdopPrHHDBbAGGeW7XYTQwEeoRfosXzcDtiGgw97b2aqjTsNFmZk7khBEQywjYfmoDc9nUCJMZ3vbSspnYo3LarLe55mh2Np8MNJqUN9APA6XkhZCrTTDRZb1B4krgFY1sVMswg2ceqguZRvC9pqt3tUUxmSnB24N6dowfVJKhLXwHPbrkHViBv1AKAJTmEaQW2DN1fRmD9ypXxZk8GXmYtxTtrj3BiunQ4qzUCu1eGzxSREjpkFSi2ATLSSDqUwxtRz639sHM6Lav4axoJNPCHbY8pvuBKUxgnGRex8LEGM8DeEJwaJCaoy8dBw9Lz49nq5mSsXLeoC4xpTUmp47Bh7GAZtwkaNreCu74m9rcZ8Di4w1cmdsiK1NWuDh9pJ2Bv7u3EfcurHFVqCkT3P86JUbKnXeNxCypfrWsFuYNKYqmjsix82g9vWcGMmAcu5nagxD4iET86iE2tMMfZZ5vqZNvntQswJyQqv2Wc6MTh4jQx1q2qJZCQe4QdEK63meTGbZNNKMctHQbp3gRkZYNrBtxQyVtNLR8xEY8zGp85GeQKbb37vqLXxRpGiigAdMe3XZA4hhYPmAAU5hpSMYaRAjtvvMT3bNiHRACGrfjvSsEG9G2zY5in2YWz5X9zXQLGTYRsQ4uNFkYoQRCBdjNxGv6R58Xq74zCgt19TxYZ87gPWxkXpWwTaHogG1eps8WXt8QzwJ9rVx6Vu9a5GjtcGsQxHovWmYixgBU8X9fPNJ9UQhYyAWbjtRSuVBtDAmoV1gCBEPwnYVP5GCGhCocbwoYhZkZjFZy6ws4uxVLid3FxuvhWvQrVEDYp7WRvGXbNdCbcSXnbeTrPMey1WPaXX');


-- Number of unspent boxes
create table dis.unspent_boxes (
	timestamp bigint primary key,
	boxes bigint not null
);


-- Total supply (ERG) in top x addresses - excluding coinbase, treasury, dapps and cexs
create table dis.top_addresses_supply (
	timestamp bigint primary key,
	top10 integer, 
	top100 integer,
	top1k integer,
	top10k integer,
	cexs integer,
	dapps integer
);


-- Number of addresses with balance of at least x ERG
create table dis.address_counts_by_minimal_balance (
	timestamp bigint primary key,
	total bigint not null,
	m_0_001 bigint not null,
	m_0_01 bigint not null,
	m_0_1 bigint not null,
	m_1 bigint not null,
	m_10 bigint not null,
	m_100 bigint not null,
	m_1k bigint not null,
	m_10k bigint not null,
	m_100k bigint not null,
	m_1m bigint not null
);


-- Preview of latest entry
create table dis.preview (
	singleton integer primary key default 1,
	timestamp bigint not null,
	total_addresses bigint not null,
	top100_supply_fraction numeric not null,
	boxes bigint not null,  
	check(singleton = 1)
);

-- Summary of address counts
create table dis.address_counts_summary (
	label text primary key,
	latest bigint,
	diff_1d bigint,
	diff_1w bigint,
	diff_4w bigint,
	diff_6m bigint,
	diff_1y bigint
);
