use crate::db::core::sql::outputs::OutputRow;
use crate::db::SQLStatement;
use crate::parsing::BlockData;

pub fn extract_outputs(block: &BlockData) -> Vec<SQLStatement> {
    block
        .transactions
        .iter()
        .flat_map(|tx| {
            tx.outputs.iter().map(|op| {
                OutputRow {
                    box_id: &op.box_id,
                    tx_id: &tx.id,
                    header_id: &block.header_id,
                    creation_height: op.creation_height,
                    address: &op.address,
                    index: op.index,
                    value: op.value,
                }
                .to_statement()
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::extract_outputs;
    use crate::db::core::sql;
    use crate::parsing::testing::block_600k;

    #[test]
    fn statements() -> () {
        let statements = extract_outputs(&block_600k());
        assert_eq!(statements.len(), 6);
        assert_eq!(statements[0].sql, sql::outputs::INSERT_OUTPUT);
        assert_eq!(statements[1].sql, sql::outputs::INSERT_OUTPUT);
        assert_eq!(statements[2].sql, sql::outputs::INSERT_OUTPUT);
        assert_eq!(statements[3].sql, sql::outputs::INSERT_OUTPUT);
        assert_eq!(statements[4].sql, sql::outputs::INSERT_OUTPUT);
        assert_eq!(statements[5].sql, sql::outputs::INSERT_OUTPUT);
    }
}
