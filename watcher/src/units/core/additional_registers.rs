use super::BlockData;
use crate::db::core::registers::BoxRegisterRow;
use crate::db::SQLStatement;

pub(super) fn extract_additional_registers(block: &BlockData) -> Vec<SQLStatement> {
    block
        .transactions
        .iter()
        .flat_map(|tx| {
            tx.outputs.iter().flat_map(|op| {
                op.additional_registers
                    .iter()
                    .filter(|r| r.is_some())
                    .map(|r| r.as_ref().unwrap())
                    .map(|r| {
                        BoxRegisterRow {
                            id: r.id,
                            box_id: &op.box_id,
                            stype: &r.stype,
                            serialized_value: &r.serialized_value,
                            rendered_value: &r.rendered_value,
                        }
                        .to_statement()
                    })
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::extract_additional_registers;
    use crate::db;
    use crate::units::testing::block_600k;
    #[test]

    fn register_statements() -> () {
        let statements = extract_additional_registers(&block_600k());
        assert_eq!(statements.len(), 3);
        assert_eq!(statements[0].sql, db::core::registers::INSERT_BOX_REGISTER);
        assert_eq!(statements[1].sql, db::core::registers::INSERT_BOX_REGISTER);
        assert_eq!(statements[2].sql, db::core::registers::INSERT_BOX_REGISTER);
    }
}
