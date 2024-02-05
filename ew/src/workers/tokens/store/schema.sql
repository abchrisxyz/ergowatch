create schema if not exists tokens;

create table tokens.balance_diffs (
    address_id bigint not null,
    asset_id bigint not null,
    height integer not null,
    -- Index of block transaction
    tx_idx smallint not null,
    -- Balance difference
    value bigint not null,
    primary key (address_id, asset_id, height, tx_idx)
);
create index on tokens.balance_diffs using brin(height);

create table tokens.balances (
    address_id bigint not null,
    asset_id bigint not null,
    -- Balance
    value bigint not null,
    primary key(address_id, asset_id),
    -- Balance cannot be negative and we don't keep spent addresses
    check (value > 0)
);

-- Balance logs to enable rollbacks.

-- Log changed or spent balances as they where prior to modification
-- by block at given height.
create table tokens._log_balances_previous_state_at (
	height integer not null,
	address_id bigint not null,
    asset_id bigint not null,
	value bigint not null,
	primary key(height, address_id, asset_id)
);
create index on tokens._log_balances_previous_state_at(height);

-- Logs address id's for which a balance was created at given height.
create table tokens._log_balances_created_at (
	height integer not null,
	address_id bigint not null,
    asset_id bigint not null,
    primary key(height, address_id, asset_id)
);
create index on tokens._log_balances_created_at(height);
