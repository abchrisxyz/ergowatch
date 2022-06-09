/// Migration 10
///
/// Remove on delete cascade clauses of some FK's
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    let statements = vec![
        "
        alter table core.transactions
            drop constraint transactions_header_id_fkey,
            add foreign key (header_id) references core.headers(id);",
        "
        alter table core.outputs
            drop constraint outputs_header_id_fkey,
            add foreign key (header_id) references core.headers(id),
            drop constraint outputs_tx_id_fkey,
            add foreign key (tx_id) references core.transactions(id);",
        "
        alter table core.inputs
            drop constraint inputs_header_id_fkey,
            add foreign key (header_id) references core.headers(id),
            drop constraint inputs_tx_id_fkey,
            add foreign key (tx_id) references core.transactions(id);",
        "
        alter table core.data_inputs
            drop constraint data_inputs_header_id_fkey,
            add foreign key (header_id) references core.headers(id),
            drop constraint data_inputs_tx_id_fkey,
            add foreign key (tx_id) references core.transactions(id),
            drop constraint data_inputs_box_id_fkey,
            add foreign key (box_id) references core.outputs(box_id);",
        "
        alter table core.box_assets
            drop constraint box_assets_box_id_fkey,
            add foreign key (box_id) references core.outputs(box_id);",
        "
        alter table core.tokens
            drop constraint tokens_box_id_fkey,
            add foreign key (box_id) references core.outputs(box_id);",
        "
        alter table core.box_registers
        drop constraint box_registers_box_id_fkey,
        add foreign key (box_id) references core.outputs(box_id);",
    ];

    for statement in statements {
        tx.execute(statement, &[])?;
    }

    Ok(())
}
