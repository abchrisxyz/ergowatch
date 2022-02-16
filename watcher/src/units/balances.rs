//! # balances
//!
//! Process blocks into balance tables data.

use super::BlockData;
use crate::db::SQLStatement;

pub struct BalancesUnit;

impl BalancesUnit {
    pub fn prep(&self, block: &BlockData) -> Vec<SQLStatement> {
        todo!();
    }
}

///
fn extract_transferred_value(block: &BlockData) -> Vec<SQLStatement> {
    // let tx = block.transactions[0];
    vec![]
}

#[cfg(test)]
mod tests {
    use super::extract_transferred_value;
    use crate::db;
    use crate::units::testing::block_600k;

    #[test]
    fn statements() -> () {
        let statements = extract_transferred_value(&block_600k());
        assert_eq!(statements.len(), 0);
        // assert_eq!(statements[0].sql, db::bal::INSERT_ERG_DIFF);
    }
}
