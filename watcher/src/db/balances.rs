//! # balances
//!
//! Process blocks into balance tables data.

// TODO undo pub once genesis refactoring is done
pub mod erg;
pub mod erg_diffs;

use super::SQLStatement;
use crate::parsing::BlockData;

pub fn prep(block: &BlockData) -> Vec<SQLStatement> {
    let mut sql_statements: Vec<SQLStatement> = block
        .transactions
        .iter()
        .map(|tx| erg_diffs::ErgDiffQuery { tx_id: &tx.id }.to_statement())
        .collect();
    sql_statements.push(erg::update_statement(block.height));
    sql_statements.push(erg::insert_statement(block.height));
    sql_statements.push(erg::delete_zero_balances_statement());
    sql_statements
}

pub fn prep_rollback(block: &BlockData) -> Vec<SQLStatement> {
    let mut sql_statements: Vec<SQLStatement> = vec![];
    sql_statements.push(erg::rollback_delete_zero_balances_statement(block.height));
    sql_statements.push(erg::rollback_update_statement(block.height));
    sql_statements.push(erg::delete_zero_balances_statement());
    sql_statements.append(
        &mut block
            .transactions
            .iter()
            .map(|tx| erg_diffs::rollback_statement(&tx.id))
            .collect(),
    );
    sql_statements
}

pub fn prep_bootstrap(height: i32) -> Vec<SQLStatement> {
    vec![
        erg_diffs::bootstrapping::insert_diffs_statement(height),
        erg::update_statement(height),
        erg::insert_statement(height),
        erg::delete_zero_balances_statement(),
    ]
}

#[cfg(test)]
mod tests {
    use super::erg;
    use super::erg_diffs;
    use crate::db::SQLArg;
    use crate::parsing::testing::block_600k;
    use pretty_assertions::assert_eq;

    #[test]
    fn check_prep_statements() -> () {
        let statements = super::prep(&block_600k());
        assert_eq!(statements.len(), 6);
        assert_eq!(statements[0].sql, erg_diffs::INSERT_DIFFS);
        assert_eq!(statements[1].sql, erg_diffs::INSERT_DIFFS);
        assert_eq!(statements[2].sql, erg_diffs::INSERT_DIFFS);
        assert_eq!(statements[3].sql, erg::UPDATE_BALANCES);
        assert_eq!(statements[4].sql, erg::INSERT_BALANCES);
        assert_eq!(statements[5].sql, erg::DELETE_ZERO_BALANCES);
    }

    #[test]
    fn check_rollback_statements() -> () {
        let statements = super::prep_rollback(&block_600k());
        assert_eq!(statements.len(), 6);
        assert_eq!(statements[0].sql, erg::ROLLBACK_DELETE_ZERO_BALANCES);
        assert_eq!(statements[1].sql, erg::ROLLBACK_BALANCE_UPDATES);
        assert_eq!(statements[2].sql, erg::DELETE_ZERO_BALANCES);
        assert_eq!(statements[3].sql, erg_diffs::DELETE_DIFFS);
        assert_eq!(statements[4].sql, erg_diffs::DELETE_DIFFS);
        assert_eq!(statements[5].sql, erg_diffs::DELETE_DIFFS);
    }

    #[test]
    fn check_bootstrap_statements() -> () {
        let statements = super::prep_bootstrap(600000);
        assert_eq!(statements.len(), 4);
        assert_eq!(
            statements[0].sql,
            erg_diffs::bootstrapping::INSERT_DIFFS_AT_HEIGHT
        );
        assert_eq!(statements[1].sql, erg::UPDATE_BALANCES);
        assert_eq!(statements[2].sql, erg::INSERT_BALANCES);
        assert_eq!(statements[3].sql, erg::DELETE_ZERO_BALANCES);

        assert_eq!(statements[0].args[0], SQLArg::Integer(600000));
        assert_eq!(statements[1].args[0], SQLArg::Integer(600000));
        assert_eq!(statements[2].args[0], SQLArg::Integer(600000));
        assert_eq!(statements[3].args.len(), 0);
    }
}
