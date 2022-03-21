use super::sql::data_inputs::DataInputRow;
use crate::db::SQLStatement;
use crate::parsing::BlockData;

pub(super) fn extract_data_inputs(block: &BlockData) -> Vec<SQLStatement> {
    block
        .transactions
        .iter()
        .flat_map(|tx| {
            tx.data_input_box_ids.iter().enumerate().map(|(ix, id)| {
                DataInputRow {
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
    use super::extract_data_inputs;
    use crate::db;
    use crate::parsing::testing::block_600k;

    #[test]
    fn statements() -> () {
        let statements = extract_data_inputs(&block_600k());
        assert_eq!(statements.len(), 1);
        assert_eq!(
            statements[0].sql,
            db::core::sql::data_inputs::INSERT_DATA_INPUT
        );
    }
}
