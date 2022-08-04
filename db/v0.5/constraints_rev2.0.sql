/*****************************************************************************
	This is a snapshot of the db schema that shipped with v0.4, prior to any
	migrations.	It's sole purpose is to test db migrations.
*****************************************************************************/

-------------------------------------------------------------------------------
-- Core
------------------------------------------------------------------------------- 
alter table core.addresses add primary key (id);
alter table core.addresses alter column id set not null;
alter table core.addresses alter column address set not null;
create index on core.addresses (md5(address));
alter table core.addresses add exclude using hash (address with=);

alter table core.headers add primary key (height);
alter table core.headers alter column id set not null;
alter table core.headers alter column parent_id set not null;
alter table core.headers alter column timestamp set not null;
alter table core.headers alter column difficulty set not null;
alter table core.headers alter column vote1 set not null;
alter table core.headers alter column vote2 set not null;
alter table core.headers alter column vote3 set not null;
alter table core.headers add constraint headers_unique_id unique(id);
alter table core.headers add constraint headers_unique_parent_id unique(parent_id);

alter table core.transactions add primary key (id);
alter table core.transactions add foreign key (header_id)
	references core.headers (id)
	on delete cascade;
create index on core.transactions(height);

alter table core.outputs add primary key (box_id);
alter table core.outputs alter column tx_id set not null;
alter table core.outputs alter column header_id set not null;
alter table core.outputs alter column address_id set not null;
alter table core.outputs alter column size set not null;
alter table core.outputs add foreign key (tx_id)
	references core.transactions (id)
	on delete cascade;
alter table core.outputs add foreign key (header_id)
	references core.headers (id)
	on delete cascade;
alter table core.outputs add foreign key (address_id) references core.addresses (id);
create index on core.outputs(tx_id);
create index on core.outputs(header_id);
create index on core.outputs(address_id);
create index on core.outputs(index);

alter table core.inputs add primary key (box_id);
alter table core.inputs alter column tx_id set not null;
alter table core.inputs alter column header_id set not null;
alter table core.inputs add foreign key (tx_id)
	references core.transactions (id)
	on delete cascade;
alter table core.inputs add foreign key (header_id)
	references core.headers (id)
	on delete cascade;
-- Not applicable to genesis block
-- alter table core.inputs foreign key (box_id)
-- 	references core.outputs (box_id)
-- 	on delete cascade;
create index on core.inputs(tx_id);
create index on core.inputs(header_id);
create index on core.inputs(index);

alter table core.data_inputs add primary key (box_id, tx_id);
alter table core.data_inputs alter column header_id set not null;
alter table core.data_inputs add foreign key (tx_id)
	references core.transactions (id)
	on delete cascade;
alter table core.data_inputs add foreign key (header_id)
	references core.headers (id)
	on delete cascade;
alter table core.data_inputs add foreign key (box_id)
	references core.outputs (box_id)
	on delete cascade;
create index on core.data_inputs(tx_id);
create index on core.data_inputs(header_id);

alter table core.box_registers add primary key (id, box_id);
alter table core.box_registers add foreign key (box_id)
	references core.outputs (box_id)
	on delete cascade;
alter table core.box_registers add check (id >= 4 and id <= 9);

alter table core.tokens add primary key (id, box_id);
alter table core.tokens alter column box_id set not null;
alter table core.tokens	add foreign key (box_id)
	references core.outputs (box_id)
	on delete cascade;
alter table core.tokens add check (emission_amount > 0);

alter table core.box_assets add primary key (box_id, token_id);
alter table core.box_assets alter column box_id set not null;
alter table core.box_assets alter column token_id set not null;
alter table core.box_assets	add foreign key (box_id)
	references core.outputs (box_id)
	on delete cascade;
alter table core.box_assets add check (amount > 0);
create index on core.box_assets (box_id);

alter table core.system_parameters add primary key (height);


-------------------------------------------------------------------------------
-- Unpent boxes
-------------------------------------------------------------------------------
alter table usp.boxes add primary key (box_id);


-------------------------------------------------------------------------------
-- Balances
-------------------------------------------------------------------------------
alter table bal.erg add primary key(address_id);
alter table bal.erg alter column address_id set not null;
alter table bal.erg add check (value >= 0);
create index on bal.erg(value);

alter table bal.erg_diffs add primary key(address_id, height, tx_id);
alter table bal.erg alter column address_id set not null;
create index on bal.erg_diffs(height);

alter table bal.tokens add primary key(address_id, token_id);
alter table bal.tokens alter column address_id set not null;
alter table bal.tokens add check (value >= 0);
create index on bal.tokens(value);

alter table bal.tokens_diffs add primary key(address_id, token_id, height, tx_id);
alter table bal.tokens_diffs alter column address_id set not null;
create index on bal.tokens_diffs(height);

update bal._log set constraints_set = TRUE;


-------------------------------------------------------------------------------
-- Finally
------------------------------------------------------------------------------- 
update ew.constraints set tier_1 = true;
update ew.constraints set tier_2 = true;
