-------------------------------------------------------------------------------
-- Migrations
-------------------------------------------------------------------------------
drop schema if exists ew cascade;
create schema ew;
create table ew.revision (
	singleton int primary key default 1,
	version integer not null,
	check(singleton = 1)
);
insert into ew.revision (version) values (1);


-------------------------------------------------------------------------------
-- Core
------------------------------------------------------------------------------- 
drop schema if exists core cascade;
create schema core;

create table core.headers (
	height int,
	id text,
	parent_id text,
	timestamp bigint
);

alter table core.headers add primary key (height);
alter table core.headers alter column id set not null;
alter table core.headers alter column parent_id set not null;
alter table core.headers alter column timestamp set not null;
alter table core.headers add constraint headers_unique_id unique(id);
alter table core.headers add constraint headers_unique_parent_id unique(parent_id);


create table core.transactions (
	id text,
	header_id text,
	height integer,
	index integer
);

alter table core.transactions add primary key (id);
alter table core.transactions add constraint core_transactions_header_id__fk_core_headers_id
	foreign key (header_id)
	references core.headers (id)
	on delete cascade;


create table core.outputs (
	box_id text,
	tx_id text,
	header_id text,
	creation_height int,
	address text,
	-- settlement_height int,
	index int,
	value bigint,
	additional_registers json
);

alter table core.outputs add primary key (box_id);
alter table core.outputs alter column tx_id set not null;
alter table core.outputs alter column header_id set not null;
alter table core.outputs alter column address set not null;
alter table core.outputs add constraint core_outputs_tx_id__fk_core_transactions_id
	foreign key (tx_id)
	references core.transactions (id)
	on delete cascade;
alter table core.outputs add constraint core_outputs_header_id__fk_core_headers_id
	foreign key (header_id)
	references core.headers (id)
	on delete cascade;


create table core.inputs (
	box_id text,
	tx_id text,
	header_id text,
	index int
);

alter table core.inputs add primary key (box_id);
alter table core.inputs alter column tx_id set not null;
alter table core.inputs alter column header_id set not null;
alter table core.inputs add constraint core_inputs_tx_id__fk_core_transactions_id
	foreign key (tx_id)
	references core.transactions (id)
	on delete cascade;
alter table core.inputs add constraint core_inputs_header_id__fk_core_headers_id
	foreign key (header_id)
	references core.headers (id)
	on delete cascade;
alter table core.inputs	add constraint core_inputs_box_id__fk_core_outputs_box_id
	foreign key (box_id)
	references core.outputs (box_id)
	on delete cascade;


create table core.data_inputs (
	box_id text,
	tx_id text,
	header_id text,
	index int
);

alter table core.data_inputs add primary key (box_id);
alter table core.data_inputs alter column tx_id set not null;
alter table core.data_inputs alter column header_id set not null;
alter table core.data_inputs add constraint core_data_inputs_tx_id__fk_core_transactions_id
	foreign key (tx_id)
	references core.transactions (id)
	on delete cascade;
alter table core.data_inputs add constraint core_data_inputs_header_id__fk_core_headers_id
	foreign key (header_id)
	references core.headers (id)
	on delete cascade;
alter table core.data_inputs add constraint core_data_inputs_box_id__fk_core_outputs_box_id
	foreign key (box_id)
	references core.outputs (box_id)
	on delete cascade;


create table core.tokens (
	token_id text,
	box_id text,
	emission_amount bigint,
	name text,
	description text,
	type text,
	decimals integer
);


create table core.box_assets (
	box_id text,
	token_id text,
	value numeric
);

alter table core.box_assets add primary key (box_id, token_id);
alter table core.box_assets
	add constraint core_box_assets_box_id__fk_core_outputs_box_id
	foreign key (box_id) references core.outputs (box_id);


-------------------------------------------------------------------------------
-- Balances
------------------------------------------------------------------------------- 
drop schema if exists bal cascade;
create schema bal;

-- Changes in ERG balances
create table bal.erg (
	address text,
	height int,
	change bigint
);

-- alter table bal.erg add primary key (address, height);

-- Changes in token balances
create table bal.tokens (
	address text,
	token_id text,
	height int,
	value numeric
);