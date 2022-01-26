use super::BlockData;
use crate::db::core::transaction::TransactionRow;
use crate::db::SQLStatement;

// Convert block transactions to sql statements
pub(super) fn extract_transactions(block: &BlockData) -> Vec<SQLStatement> {
    block
        .transactions
        .iter()
        .map(|tx| TransactionRow {
            id: &tx.id,
            header_id: &block.header_id,
            height: block.height,
            index: tx.index,
        })
        .map(|row| row.to_statement())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::extract_transactions;
    use crate::db;
    use crate::units::testing::block_600k;

    #[test]
    fn statements() -> () {
        let statements = extract_transactions(&block_600k());
        assert_eq!(statements.len(), 3);
        assert_eq!(statements[0].sql, db::core::transaction::INSERT_TRANSACTION);
        assert_eq!(statements[1].sql, db::core::transaction::INSERT_TRANSACTION);
        assert_eq!(statements[2].sql, db::core::transaction::INSERT_TRANSACTION);
    }
}
