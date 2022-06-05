/// Migration 8
///
/// Back process SUnit register values.
/// https://github.com/abchrisxyz/ergowatch/issues/25
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute(
        "
        update core.box_registers
        set value_type = 'SUnit',
            rendered_value = '()'
        where serialized_value = '62';
        ",
        &[],
    )?;

    Ok(())
}
