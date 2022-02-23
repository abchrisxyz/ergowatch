-------------------------------------------------------------------------------
-- Core
------------------------------------------------------------------------------- 
alter table core.headers add primary key (height);
alter table core.headers alter column id set not null;
alter table core.headers alter column parent_id set not null;
alter table core.headers alter column timestamp set not null;
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
alter table core.outputs alter column address set not null;
alter table core.outputs add foreign key (tx_id)
	references core.transactions (id)
	on delete cascade;
alter table core.outputs add foreign key (header_id)
	references core.headers (id)
	on delete cascade;
create index on core.outputs(tx_id);
create index on core.outputs(header_id);
create index on core.outputs(address);
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


-------------------------------------------------------------------------------
-- Unpent boxes
-------------------------------------------------------------------------------
alter table usp.boxes add primary key (box_id);


-------------------------------------------------------------------------------
-- Balances
-------------------------------------------------------------------------------
alter table bal.erg add primary key(address);
alter table bal.erg add check (value >= 0);
create index on bal.erg(value);

alter table bal.erg_diffs add primary key(address, height, tx_id);

alter table bal.tokens add primary key(address, token_id);
alter table bal.tokens add check (value >= 0);
create index on bal.tokens(value);

alter table bal.tokens_diffs add primary key(address, token_id, height, tx_id);


-------------------------------------------------------------------------------
-- Finally
------------------------------------------------------------------------------- 
update ew.revision set constraints_set = true;
