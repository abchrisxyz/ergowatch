/*****************************************************************************
 *
 * DO NOT LOAD THIS - YOU ONLY NEED SCHEMA.SQL
 *
 * Constraints and indexes are set automatically by the watcher.
 *
 * This file is only used to set up test db's for integration tests
 * and provides an overview of all constraints and indexes.
 *
 *****************************************************************************/


-------------------------------------------------------------------------------
-- Core
------------------------------------------------------------------------------- 
alter table core.headers add primary key (height);
alter table core.headers alter column height set not null;
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
alter table core.transactions alter column id set not null;
alter table core.transactions alter column header_id set not null;
alter table core.transactions alter column height set not null;
alter table core.transactions alter column index set not null;
alter table core.transactions add foreign key (header_id) references core.headers (id);
create index on core.transactions(height);

alter table core.outputs add primary key (box_id);
alter table core.outputs alter column box_id set not null;
alter table core.outputs alter column tx_id set not null;
alter table core.outputs alter column header_id set not null;
alter table core.outputs alter column creation_height set not null;
alter table core.outputs alter column address set not null;
alter table core.outputs alter column index set not null;
alter table core.outputs alter column value set not null;
alter table core.outputs alter column size set not null;
alter table core.outputs add foreign key (tx_id) references core.transactions (id);
alter table core.outputs add foreign key (header_id) references core.headers (id);
create index on core.outputs(tx_id);
create index on core.outputs(header_id);
create index on core.outputs(address);
create index on core.outputs(index);

alter table core.inputs add primary key (box_id);
alter table core.inputs alter column box_id set not null;
alter table core.inputs alter column tx_id set not null;
alter table core.inputs alter column header_id set not null;
alter table core.inputs alter column index set not null;
alter table core.inputs add foreign key (tx_id) references core.transactions (id);
alter table core.inputs add foreign key (header_id) references core.headers (id);
-- Not applicable to genesis block
-- alter table core.inputs foreign key (box_id) references core.outputs (box_id);
create index on core.inputs(tx_id);
create index on core.inputs(header_id);
create index on core.inputs(index);

alter table core.data_inputs add primary key (box_id, tx_id);
alter table core.data_inputs alter column box_id set not null;
alter table core.data_inputs alter column tx_id set not null;
alter table core.data_inputs alter column header_id set not null;
alter table core.data_inputs alter column index set not null;
alter table core.data_inputs add foreign key (tx_id) references core.transactions (id);
alter table core.data_inputs add foreign key (header_id) references core.headers (id);
alter table core.data_inputs add foreign key (box_id) references core.outputs (box_id);
create index on core.data_inputs(tx_id);
create index on core.data_inputs(header_id);

alter table core.box_registers add primary key (id, box_id);
alter table core.box_registers alter column id set not null;
alter table core.box_registers alter column box_id set not null;
alter table core.box_registers alter column value_type set not null;
alter table core.box_registers alter column serialized_value set not null;
alter table core.box_registers alter column rendered_value set not null;
alter table core.box_registers add foreign key (box_id) references core.outputs (box_id);
alter table core.box_registers add check (id >= 4 and id <= 9);

alter table core.tokens add primary key (id, box_id);
alter table core.tokens alter column id set not null;
alter table core.tokens alter column box_id set not null;
alter table core.tokens alter column emission_amount set not null;
alter table core.tokens	add foreign key (box_id) references core.outputs (box_id);
alter table core.tokens add check (emission_amount > 0);

alter table core.box_assets add primary key (box_id, token_id);
alter table core.box_assets alter column box_id set not null;
alter table core.box_assets alter column token_id set not null;
alter table core.box_assets alter column amount set not null;
alter table core.box_assets	add foreign key (box_id) references core.outputs (box_id);
alter table core.box_assets add check (amount > 0);

alter table core.system_parameters add primary key (height);


-------------------------------------------------------------------------------
-- Unpent boxes
-------------------------------------------------------------------------------
alter table usp.boxes add primary key (box_id);
alter table usp.boxes alter column box_id set not null;


-------------------------------------------------------------------------------
-- Balances
-------------------------------------------------------------------------------
alter table bal.erg add primary key(address);
alter table bal.erg alter column address set not null;
alter table bal.erg alter column value set not null;
alter table bal.erg add check (value >= 0);
create index on bal.erg(value);

alter table bal.erg_diffs add primary key(address, height, tx_id);
alter table bal.erg_diffs alter column address set not null;
alter table bal.erg_diffs alter column height set not null;
alter table bal.erg_diffs alter column tx_id set not null;
alter table bal.erg_diffs alter column value set not null;
create index on bal.erg_diffs(height);

alter table bal.tokens add primary key(address, token_id);
alter table bal.tokens alter column address set not null;
alter table bal.tokens alter column token_id set not null;
alter table bal.tokens alter column value set not null;
alter table bal.tokens add check (value >= 0);
create index on bal.tokens(value);

alter table bal.tokens_diffs add primary key(address, token_id, height, tx_id);
alter table bal.tokens_diffs alter column address set not null;
alter table bal.tokens_diffs alter column token_id set not null;
alter table bal.tokens_diffs alter column height set not null;
alter table bal.tokens_diffs alter column tx_id set not null;
alter table bal.tokens_diffs alter column value set not null;
create index on bal.tokens_diffs(height);


-------------------------------------------------------------------------------
-- CEX's
-------------------------------------------------------------------------------
alter table cex.cexs add primary key (id);
alter table cex.cexs alter column id set not null;
alter table cex.cexs alter column text_id set not null;
alter table cex.cexs alter column name set not null;
alter table cex.cexs add constraint cexs_unique_text_id unique (text_id);
alter table cex.cexs add constraint cexs_unique_name unique (name);

alter table cex.addresses add primary key (address);
alter table cex.addresses alter column address set not null;
alter table cex.addresses alter column cex_id set not null;
alter table cex.addresses alter column type set not null;
alter table cex.addresses add foreign key (cex_id)
	references cex.cexs (id);
create index on cex.addresses(cex_id);
create index on cex.addresses(type);
create index on cex.addresses(spot_height);

alter table cex.addresses_ignored add primary key (address);
alter table cex.addresses_ignored alter column address set not null;

alter table cex.addresses_conflicts add primary key (address);
alter table cex.addresses_conflicts alter column address set not null;
alter table cex.addresses_conflicts alter column first_cex_id set not null;
alter table cex.addresses_conflicts alter column type set not null;
alter table cex.addresses_conflicts add foreign key (first_cex_id)
	references cex.cexs (id);

alter table cex.block_processing_log add primary key (header_id);
alter table cex.block_processing_log alter column header_id set not null;
alter table cex.block_processing_log alter column height set not null;
alter table cex.block_processing_log alter column status set not null;
create index on cex.block_processing_log (status);

alter table cex.supply add primary key (height, cex_id);
alter table cex.supply alter column height set not null;
alter table cex.supply alter column cex_id set not null;
alter table cex.supply alter column main set not null;
alter table cex.supply alter column deposit set not null;
alter table cex.supply add foreign key (cex_id)
	references cex.cexs (id);
create index on cex.supply (height);
alter table cex.supply add check (main >= 0);
alter table cex.supply add check (deposit >= 0);

-------------------------------------------------------------------------------
-- Metrics
-------------------------------------------------------------------------------
-- CEX's
alter table mtr.cex_supply add primary key (height);
alter table mtr.cex_supply alter column height set not null;
alter table mtr.cex_supply alter column total set not null;
alter table mtr.cex_supply alter column deposit set not null;
alter table mtr.cex_supply add check (total >= 0);
alter table mtr.cex_supply add check (deposit >= 0);

-- UTxO's
alter table mtr.utxos add primary key(height);
