/// Migration 6
///
/// Add additional constraints
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    let statements = vec![
        "alter table core.headers alter column height set not null;",
        "alter table core.transactions alter column id set not null;",
        "alter table core.transactions alter column header_id set not null;",
        "alter table core.transactions alter column height set not null;",
        "alter table core.transactions alter column index set not null;",
        "alter table core.outputs alter column box_id set not null;",
        "alter table core.outputs alter column creation_height set not null;",
        "alter table core.outputs alter column index set not null;",
        "alter table core.outputs alter column value set not null;",
        "alter table core.inputs alter column box_id set not null;",
        "alter table core.inputs alter column index set not null;",
        "alter table core.data_inputs alter column box_id set not null;",
        "alter table core.data_inputs alter column tx_id set not null;",
        "alter table core.data_inputs alter column index set not null;",
        "alter table core.box_registers alter column id set not null;",
        "alter table core.box_registers alter column box_id set not null;",
        "alter table core.box_registers alter column value_type set not null;",
        "alter table core.box_registers alter column serialized_value set not null;",
        "alter table core.box_registers alter column rendered_value set not null;",
        "alter table core.tokens alter column id set not null;",
        "alter table core.tokens alter column emission_amount set not null;",
        "alter table core.box_assets alter column amount set not null;",
        "alter table usp.boxes alter column box_id set not null;",
        "alter table bal.erg alter column value set not null;",
        "alter table bal.erg_diffs alter column height set not null;",
        "alter table bal.erg_diffs alter column tx_id set not null;",
        "alter table bal.erg_diffs alter column value set not null;",
        "alter table bal.tokens alter column token_id set not null;",
        "alter table bal.tokens alter column value set not null;",
        "alter table bal.tokens_diffs alter column token_id set not null;",
        "alter table bal.tokens_diffs alter column height set not null;",
        "alter table bal.tokens_diffs alter column tx_id set not null;",
        "alter table bal.tokens_diffs alter column value set not null;",
        "alter table cex.cexs alter column id set not null;",
        "alter table cex.cexs alter column name set not null;",
        "alter table cex.addresses alter column address_id set not null;",
        "alter table cex.addresses alter column cex_id set not null;",
        "alter table cex.addresses_conflicts alter column address_id set not null;",
        "alter table cex.addresses_conflicts alter column first_cex_id set not null;",
        "alter table cex.addresses_conflicts alter column type set not null;",
        "alter table cex.block_processing_log alter column header_id set not null;",
        "alter table cex.block_processing_log alter column height set not null;",
        "alter table cex.block_processing_log alter column status set not null;",
        "alter table cex.supply alter column height set not null;",
        "alter table cex.supply alter column cex_id set not null;",
        "alter table cex.supply alter column main set not null;",
        "alter table cex.supply alter column deposit set not null;",
    ];

    for statement in statements {
        tx.execute(statement, &[])?;
    }

    Ok(())
}
