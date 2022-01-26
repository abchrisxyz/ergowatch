use super::BlockData;
use crate::db::core::inputs::InputRow;
use crate::db::SQLStatement;

pub(super) fn extract_inputs(block: &BlockData) -> Vec<SQLStatement> {
    block
        .transactions
        .iter()
        .flat_map(|tx| {
            tx.input_box_ids.iter().enumerate().map(|(ix, id)| {
                InputRow {
                    box_id: &id,
                    tx_id: &tx.id,
                    header_id: &block.header_id,
                    index: ix as i32,
                }
                .to_statement()
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::extract_inputs;
    use crate::db;
    use crate::units::testing::block_600k;

    #[test]
    fn statements() -> () {
        let statements = extract_inputs(&block_600k());
        assert_eq!(statements.len(), 4);
        assert_eq!(statements[0].sql, db::core::inputs::INSERT_INPUT);
        assert_eq!(statements[1].sql, db::core::inputs::INSERT_INPUT);
        assert_eq!(statements[2].sql, db::core::inputs::INSERT_INPUT);
        assert_eq!(statements[3].sql, db::core::inputs::INSERT_INPUT);
    }
}
