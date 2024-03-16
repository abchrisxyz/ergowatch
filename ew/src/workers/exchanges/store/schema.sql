create schema exchanges;
comment on schema exchanges is 'CEX addresses and balances';

create table exchanges.exchanges (
	id integer,
	text_id text, -- used for easier api access
	name text
	-- TODO: Height at time of listing. Used to weed out deposit false positives.
	-- listing_height integer not null
);

insert into exchanges.exchanges (id, name, text_id) values
	(1, 'Coinex', 'coinex'),
	(2, 'Gate.io', 'gate'),
	(3, 'KuCoin', 'kucoin'),
	(4, 'ProBit', 'probit'),
	(5, 'TradeOgre', 'tradeogre'),
	(6, 'Huobi', 'huobi'),
	(7, 'Xeggex', 'xeggex'),
	(8, 'SevenSeas', 'sevenseas'),
	(9, 'NonKYC', 'nonkyc');

-- Known main addresses for each CEX
-- New Coinex main: 9i51m3reWk99iw8WF6PgxbUT6ZFKhzJ1PmD11vEuGu125hRaKAH
create table exchanges.main_addresses (
    address_id bigint primary key,
	cex_id integer not null,
	address text not null -- including this here to assert we have the right id's
);

-- Deposit addresses: addresses sending to main addresses of a single CEX.
create table exchanges.deposit_addresses (
	address_id bigint primary key,
	cex_id integer not null,
	spot_height integer not null
);

-- List of addresses to be ignored as deposit address.
-- Mostly addresses sending to CEX main address but active before the listing date.
create table exchanges.deposit_addresses_ignored (
	address_id bigint
);

-- Conflicts - false positives sending to more than one CEX and needing to be exlcuded.
create table exchanges.deposit_addresses_excluded (
	-- Same columns as exchanges.address
	address_id bigint not null,
	first_cex_id integer, -- can be null for intra-block conflicts
	deposit_spot_height integer not null,
	conflict_spot_height integer not null
);

-- Supply across all tracked exchanges
create table exchanges.supply (
	height integer primary key,
	-- Supply on main addresses
	main bigint not null,
	-- Supply on deposit addresses
	deposits bigint not null,
	check(main >= 0),
	check(deposits >= 0)
);

-----------------------------------------------------------------------------------------
-- Static data
-----------------------------------------------------------------------------------------
insert into exchanges.main_addresses (cex_id, address_id, address) values
	-- Coinex
	(1, 2857451, '9fowPvQ2GXdmhD2bN54EL9dRnio3kBQGyrD3fkbHwuTXD6z1wBU'),
	(1,  712861, '9fPiW45mZwoTxSwTLLXaZcdekqi72emebENmScyTGsjryzrntUe'),
	-- https://explorer.ergoplatform.com/en/transactions/cb8cdbd507fbec52a3e4509258d7e2163d3e8ecfac04977307d8b168668acded
	-- 9fPiW sends 2.9M to what looks like a deposit address 9gwU.
	-- 9gwU sends the 2.9M to Gate's 9iKFB which then sends it to 9i51m.
	-- Looks like Coinex sent funds to Gate during a security incident.
	(1, 8980751, '9i51m3reWk99iw8WF6PgxbUT6ZFKhzJ1PmD11vEuGu125hRaKAH'),
	(1, 9075411, '9fNqgokdacnYykMZmtqjTnCbBJG9mhkifghV6Pmn6taBihUoG33'),
	(1, 9081471, '9f3iGnXcebxzv4avYCUTt6dekgPMV1t5hpHdcJ4mAfX94yAiGFv'),
	(1, 9081481, '9gPwnhhzc2tEkZHpcUeQ9J9wQWKzoLjCRbNx4qAsm2dK2RvVvib'),
	(1, 9081701, '9gLxihuwAPkkgTpctawTRz82XVm46z6o8q3vMXVeZtyq6qtruWk'),
	(1, 9081711, '9f65ghY8F5k7uKMACX7o2GfaV4EzWpsNW3gBTc23pAqTon8n7pE'),
	(1, 9120481, '9iL1tEz6ENBLtaiMaEppnsrj9HjvnRaLdRyqiBPeCW6SyUtEaxM'),
	(1, 9186521, '9h4WD9zRk7efQYM9jUYy3hrReJmYMYGen4yVeEw1SWGTuM6XNXv'),
	(1, 9213581, '9fTyfkM1NWdBJvpM3rAq2SNdJ9yfvUqjM3MBStNS2Aq5fGE9Jgg'),
	(1, 9356241, '9haE48wKvgYzc3WdBXRU9ERw2ZWWkGzJT8jGHcXvzQggftiQQdC'),
			
	-- Gate - confirmed
	(2, 872431, '9iKFBBrryPhBYVGDKHuZQW7SuLfuTdUJtTPzecbQ5pQQzD4VykC'),
	-- Gate - unconfirmed
	--
	-- Has had up to 900k, sends only to 9iKFBB, received from 2800+
	-- addresses all created in/after 10/2020.
	(2, 908431, '9gQYrh6yubA4z55u4TtsacKnaEteBEdnY4W2r5BLcFZXcQoQDcq'),
	--
	-- Sends only to 9iKFBB, received from 219 addresses all created
	-- in/after 01/2021.
	-- Receives from 9i7134 (see below).
	(2, 1579721, '9enQZco9hPuqaHvR7EpPRWvYbkDYoWu3NK7pQk8VFwgVnv5taQE'),
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
	(3, 2363571, '9hU5VUSUAmhEsTehBKDGFaFQSJx574UPoCquKBq59Ushv5XYgAu'),
	(3, 3752231, '9i8Mci4ufn8iBQhzohh4V3XM3PjiJbxuDG1hctouwV4fjW5vBi3'),
	(3, 3752261, '9guZaxPoe4jecHi6ZxtMotKUL4AzpomFf3xqXsFSuTyZoLbmUBr'),
	(3, 2406961, '9iNt6wfxSc3DSaBVp22E7g993dwKUCvbGdHoEjxF8SRqj35oXvT'),
	(3, 8408121, '9fs7HkPGY9fhN6WsHd7V7LMcuMqsgseXzNahyToxJKwHCc1zc1c'),
	(3, 8834201, '9how9k2dp67jXDnCM6TeRPKtQrToCs5MYL2JoSgyGHLXm1eHxWs'),
	(3, 8834481, '9fpUtN7d22jS3cMWeZxBbzkdnHCB46YRJ8qiiaVo2wRCkaBar1Z'),
	
	-- ProBit https://discord.com/channels/668903786361651200/896711052736233482/964541753162096680
	(4, 8771, '9eg2Rz3tGogzLaVZhG1ycPj1dJtN4Jn8ySa2mnVLJyVJryb13QB'),

	-- TradeOgre
	(5, 1410341, '9fs99SejQxDjnjwrZ13YMZZ3fwMEVXFewpWWj63nMhZ6zDf2gif'),

	-- Huobi
	(6, 6621141, '9feMGM1qwNG8NnNuk3pz4yeCGm59s2RbjFnS7DxwUxCbzUrNnJw'),

	-- Xeggex
	(7, 9336381, '9hphYTmicjazd45pz2ovoHVPz5LTq9EvXoEK9JMGsfWuMtX6eDu'),
	
	-- Seven Seas
	(8, 8327441, '9hYpa8qu3GihemMA1c4RVZuRGqcmBQKChgokFm6a81R3mFafqgi'),

	-- NonKYC.io
	(9, 8951841, '9hmS5u1Khhc4PFERTA2dGzSkDuwUrrENQEUAveF6gHj8xCi9qy3');


insert into exchanges.deposit_addresses_ignored (address_id) values
	-- 9hxFS2RkmL5Fv5DRZGwZCbsbjTU1R75Luc2t5hkUcR1x3jWzre4
	-- Flagged as Gate deposit address.
	-- Active since July 2019.
	-- Received 2.6M+ direct from treasury.
	-- Most of it goes to:
	--    - 9gNYeyfRFUipiWZ3JR1ayDMoeh28E6J7aDQosb7yrzsuGSDqzCC
	--    - 9fdVtQVggW7a2EBE6CPKXjvtBzN8WCHcMuJd2zgzx8KRqRuwJVr
	-- Only 1 tx to 9iKFBB, 50k on 1 Oct 2020
	-- https://explorer.ergoplatform.com/en/transactions/afe34ee3128ce9c4838bc64c0530322db1b3aa3c48400ac50ede3b68ad08ddd2
	(6551),

	-- 9gNYeyfRFUipiWZ3JR1ayDMoeh28E6J7aDQosb7yrzsuGSDqzCC
	-- Flagged as Gate deposit address.
	-- Active since March 2020.
	-- Received 1.5M+ from 9hxFS2 (see above)
	-- Only 1 tx to 9iKFBB: 50k on 5 Oct 2020
	-- https://explorer.ergoplatform.com/en/transactions/5e202e5e37631701db2cb0ddc839601b2da74ce7f6e826bc9244f1ada5dba92c
	(249911),

	-- 9i2oKu3bbHDksfiZjbhAgSAWW7iZecUS78SDaB46Fpt2DpUNe6M
	-- Flagged as Gate deposit address.
	-- First active May 2020.
	-- Involved in 50k tx to 9iKFBB on 5 Oct 2020 (see above).
	-- Received 2 tx from 9iKFBB:
	-- 898 ERG on 30 March 2021 - https://explorer.ergoplatform.com/en/transactions/c2d592cc688ec8d8ffa7ea22e054aca31b39578ed004fcd4cbcc11783e4739db
	-- 698 ERG on 12 April 2021 - https://explorer.ergoplatform.com/en/transactions/3dd8e7015568228336a5d16c5b690e3a5653d2d827711a9b1580e0b7db13e563
	(516491),

	-- 9iHCMtd2gAPoYGhWadjruygKwNKRoeQGq1xjS2Fkm5bT197YFdR
	-- Flagged as Gate deposit address.
	-- First active June 2020.
	-- Received 100 ERG from 9fPiW4 (Coinex withdrawal) in 2 txs in June 2020:
	--    - https://explorer.ergoplatform.com/en/transactions/6e73b4e7e1e0e339ba6185fd142ac2df8409e9bebcffed7b490107633695fe88
	--    - https://explorer.ergoplatform.com/en/transactions/34193dbdde8921b74ece7cae1adf495830a52fb72477c2a610b31cb4750b45f2
	-- Received 550 ERG from 9gNYey and others in June 2020 - https://explorer.ergoplatform.com/en/transactions/811b17a00d821763aa096dc3e6225122451b068454eb1cfb16bf5c7b47fea9f5
	-- Sent 20 ERG to 9iKFBB on 27 September 2020 - https://explorer.ergoplatform.com/en/transactions/8bc2caf976e5e5f0786ee54bb886f3344e6dac1c034491766e977c4b3a828305
	-- First ever tx to 9iKFBB (!)
	(644701);


