//! # balances
//!
//! Process blocks into balance tables data.

use super::BlockData;
// use crate::db;
use crate::db::bal;
use crate::db::SQLStatement;

pub fn prep(block: &BlockData) -> Vec<SQLStatement> {
    let mut sql_statements: Vec<SQLStatement> = block
        .transactions
        .iter()
        .map(|tx| bal::erg_diff::ErgDiffQuery { tx_id: &tx.id }.to_statement())
        .collect();
    sql_statements.push(bal::erg::update_statement(block.height));
    sql_statements.push(bal::erg::insert_statement(block.height));
    sql_statements.push(bal::erg::delete_zero_balances_statement());
    sql_statements
}

pub fn prep_rollback(block: &BlockData) -> Vec<SQLStatement> {
    let mut sql_statements: Vec<SQLStatement> = vec![];
    sql_statements.push(bal::erg::rollback_delete_zero_balances_statement(
        block.height,
    ));
    sql_statements.push(bal::erg::rollback_update_statement(block.height));
    sql_statements.push(bal::erg::delete_zero_balances_statement());
    sql_statements.append(
        &mut block
            .transactions
            .iter()
            .map(|tx| bal::erg_diff::rollback_statement(&tx.id))
            .collect(),
    );
    sql_statements
}

pub fn prep_bootstrap(height: i32) -> Vec<SQLStatement> {
    vec![
        bal::erg_diff::bootstrapping::insert_diffs_statement(height),
        bal::erg::update_statement(height),
        bal::erg::insert_statement(height),
        bal::erg::delete_zero_balances_statement(),
    ]
}

#[cfg(test)]
mod tests {
    use crate::db;
    use crate::units::testing::block_600k;
    use pretty_assertions::assert_eq;

    #[test]
    fn check_prep_statements() -> () {
        let statements = super::prep(&block_600k());
        assert_eq!(statements.len(), 6);
        assert_eq!(statements[0].sql, db::bal::erg_diff::INSERT_DIFFS);
        assert_eq!(statements[1].sql, db::bal::erg_diff::INSERT_DIFFS);
        assert_eq!(statements[2].sql, db::bal::erg_diff::INSERT_DIFFS);
        assert_eq!(statements[3].sql, db::bal::erg::UPDATE_BALANCES);
        assert_eq!(statements[4].sql, db::bal::erg::INSERT_BALANCES);
        assert_eq!(statements[5].sql, db::bal::erg::DELETE_ZERO_BALANCES);
    }

    #[test]
    fn check_rollback_statements() -> () {
        let statements = super::prep_rollback(&block_600k());
        assert_eq!(statements.len(), 6);
        assert_eq!(
            statements[0].sql,
            db::bal::erg::ROLLBACK_DELETE_ZERO_BALANCES
        );
        assert_eq!(statements[1].sql, db::bal::erg::ROLLBACK_BALANCE_UPDATES);
        assert_eq!(statements[2].sql, db::bal::erg::DELETE_ZERO_BALANCES);
        assert_eq!(statements[3].sql, db::bal::erg_diff::DELETE_DIFFS);
        assert_eq!(statements[4].sql, db::bal::erg_diff::DELETE_DIFFS);
        assert_eq!(statements[5].sql, db::bal::erg_diff::DELETE_DIFFS);
    }

    #[test]
    fn check_bootstrap_statements() -> () {
        let statements = super::prep_bootstrap(600000);
        assert_eq!(statements.len(), 4);
        assert_eq!(
            statements[0].sql,
            db::bal::erg_diff::bootstrapping::INSERT_DIFFS_AT_HEIGHT
        );
        assert_eq!(statements[1].sql, db::bal::erg::UPDATE_BALANCES);
        assert_eq!(statements[2].sql, db::bal::erg::INSERT_BALANCES);
        assert_eq!(statements[3].sql, db::bal::erg::DELETE_ZERO_BALANCES);

        assert_eq!(statements[0].args[0], db::SQLArg::Integer(600000));
        assert_eq!(statements[1].args[0], db::SQLArg::Integer(600000));
        assert_eq!(statements[2].args[0], db::SQLArg::Integer(600000));
        assert_eq!(statements[3].args.len(), 0);
    }
}
