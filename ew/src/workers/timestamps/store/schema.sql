create schema timestamps;

-- Timestamps of each block
create table timestamps.timestamps (
    height integer primary key,
    timestamp bigint not null
);

-- Hourly timestamps
-- Holds height of last block at each round hour.
-- Starts at genesis, ends with last block
create table timestamps.hourly (
    height integer not null,
    timestamp bigint not null
);
create index on timestamps.hourly using brin(height);
create index on timestamps.hourly using brin(timestamp);

-- Daily timestamps
-- Holds height of last block at each midnight UTC.
-- Starts at genesis, ends with last block
create table timestamps.daily (
    height integer not null,
    timestamp bigint not null
);
create index on timestamps.daily using brin(height);
create index on timestamps.daily using brin(timestamp);

-- Weekly timestamps
-- Holds height of last block at each Monday midnight UTC (right after Sunday).
create table timestamps.weekly (
    height integer not null,
    timestamp bigint not null
-- Starts at genesis, ends with last block
);
create index on timestamps.weekly using brin(height);
create index on timestamps.weekly using brin(timestamp);
