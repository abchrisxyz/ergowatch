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
alter table core.transactions add constraint core_transactions_header_id__fk_core_headers_id
	foreign key (header_id)
	references core.headers (id)
	on delete cascade;

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
-- Not applicable to genesis block
-- alter table core.inputs add constraint core_inputs_box_id__fk_core_outputs_box_id
-- 	foreign key (box_id)
-- 	references core.outputs (box_id)
-- 	on delete cascade;

alter table core.data_inputs add primary key (box_id, tx_id);
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

alter table core.box_registers add primary key (id, box_id);
alter table core.box_registers add constraint core_box_registers_box_id__fk_core_outputs_box_id
	foreign key (box_id)
	references core.outputs (box_id)
	on delete cascade;
alter table core.box_registers add constraint core_box_registers__id_range_check
	check (id >= 4 and id <= 9);

alter table core.tokens add primary key (id, box_id);
alter table core.tokens alter column box_id set not null;
alter table core.tokens	add constraint core_tokens_box_id__fk_core_outputs_box_id
	foreign key (box_id)
	references core.outputs (box_id)
	on delete cascade;
alter table core.tokens add constraint core_tokens__positive_emission_amount_check
	check (emission_amount > 0);

alter table core.box_assets add primary key (box_id, token_id);
alter table core.box_assets alter column box_id set not null;
alter table core.box_assets alter column token_id set not null;
alter table core.box_assets	add constraint core_box_assets_box_id__fk_core_outputs_box_id
	foreign key (box_id)
	references core.outputs (box_id)
	on delete cascade;
alter table core.box_assets add constraint core_box_assets__positive_amount_check
	check (amount > 0);
