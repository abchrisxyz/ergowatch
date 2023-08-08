create schema core;

create table core._meta (
	singleton int primary key default 1,
	rev_major integer not null,
	rev_minor integer not null,
    height integer not null default -1,
	check(singleton = 1)
);
insert into core._meta (rev_major, rev_minor) values (1, 0);

create table core.headers (
    height integer primary key not null,
    timestamp bigint not null,
    id text not null
);

-- Composite type representing a token balance
create type asset as (
	asset_id bigint,
	amount bigint
);

create table core.boxes (
	box_id varchar(64) collate "C" primary key not null,
	height integer not null,
	creation_height integer not null,
	address_id bigint not null,
	value bigint not null,
	size integer not null,
	assets asset[], -- null when no assets
	registers json not null
);
create index on core.boxes using brin(height);

create table core.addresses (
	id bigint primary key not null,
	spot_height int not null,
	address text not null
);
-- Addresses can exceed max indexable length so we index their hash instead
create index on core.addresses (md5(address));
alter table core.addresses add exclude using hash (address with=);
create index on core.addresses using brin(spot_height);

-- Helper function to obtain address id from plain address.
create function core.address_id(_address text) returns bigint as '
	select id
	from core.addresses
	where md5(address) = md5($1)
		and address = $1;'
    language sql
    immutable
    returns null on null input;

create table core.tokens (
	asset_id bigint primary key not null,
	spot_height integer not null,
	token_id varchar(64) not null
);
create index on core.tokens(token_id);
create index on core.addresses using brin(spot_height);

-- create table core.transactions (
--     id bigint primary key not null,
--     spot_height integer not null,
--     base16_id text not null
-- );
-- create index on core.transactions using brin(spot_height);

-- Don't need metadata for all tokens if using token-specific processing units
-- core.tokens (
--     id bigint not null primary key,
--     spot_height integer not null,
--     base16_id text not null,
--     emission_amount bigint,
-- 	name text,
-- 	description text,
-- 	decimals integer,
-- 	standard text
-- );
-- create index on core.tokens using brin(spot_height);
