create schema coingecko;

create table coingecko.ergusd_hourly (
    timestamp bigint primary key,
    value real not null
);

create table coingecko.ergusd_block (
    height integer primary key,
    value real not null
);

-- Height and timestamp of block records that haven't been interpolated yet.
-- Those will have the latest hourly value that was available when they were
-- created and will get updated as more hourly data becomes available.
create table coingecko.ergusd_provisional_blocks (
	height integer primary key,
    timestamp bigint not null
);
