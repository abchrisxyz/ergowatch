//! # balances
//!
//! Process blocks into balance tables data.

// TODO undo pub once genesis refactoring is done
pub mod erg;
pub mod erg_diffs;
pub mod tokens;
pub mod tokens_diffs;

use super::SQLStatement;
use crate::parsing::BlockData;

pub fn prep(block: &BlockData) -> Vec<SQLStatement> {
    let tx_ids: Vec<&str> = block.transactions.iter().map(|tx| tx.id).collect();
    let mut sql_statements: Vec<SQLStatement> = Vec::new();
    // Erg
    sql_statements.append(
        &mut tx_ids
            .iter()
            .map(|tx_id| erg_diffs::ErgDiffQuery { tx_id: &tx_id }.to_statement())
            .collect(),
    );
    sql_statements.push(erg::update_statement(block.height));
    sql_statements.push(erg::insert_statement(block.height));
    sql_statements.push(erg::delete_zero_balances_statement());
    // Tokens
    sql_statements.append(
        &mut tx_ids
            .iter()
            .map(|tx_id| tokens_diffs::TokenDiffQuery { tx_id: &tx_id }.to_statement())
            .collect(),
    );
    sql_statements.push(tokens::update_statement(block.height));
    sql_statements.push(tokens::insert_statement(block.height));
    sql_statements.push(tokens::delete_zero_balances_statement());

    sql_statements
}

pub fn prep_rollback(block: &BlockData) -> Vec<SQLStatement> {
    let mut sql_statements: Vec<SQLStatement> = vec![];
    // Tokens
    sql_statements.push(tokens::rollback_delete_zero_balances_statement(
        block.height,
    ));
    sql_statements.push(tokens::rollback_update_statement(block.height));
    sql_statements.push(tokens::delete_zero_balances_statement());
    sql_statements.append(
        &mut block
            .transactions
            .iter()
            .map(|tx| tokens_diffs::rollback_statement(&tx.id))
            .collect(),
    );
    // Erg
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
        tokens_diffs::bootstrapping::insert_diffs_statement(height),
        tokens::update_statement(height),
        tokens::insert_statement(height),
        tokens::delete_zero_balances_statement(),
    ]
}

#[cfg(test)]
mod tests {
    use super::erg;
    use super::erg_diffs;
    use super::tokens;
    use super::tokens_diffs;
    use crate::db::SQLArg;
    use crate::parsing::testing::block_600k;
    use pretty_assertions::assert_eq;

    #[test]
    fn check_prep_statements() -> () {
        let statements = super::prep(&block_600k());
        assert_eq!(statements.len(), 12);
        assert_eq!(statements[0].sql, erg_diffs::INSERT_DIFFS);
        assert_eq!(statements[1].sql, erg_diffs::INSERT_DIFFS);
        assert_eq!(statements[2].sql, erg_diffs::INSERT_DIFFS);
        assert_eq!(statements[3].sql, erg::UPDATE_BALANCES);
        assert_eq!(statements[4].sql, erg::INSERT_BALANCES);
        assert_eq!(statements[5].sql, erg::DELETE_ZERO_BALANCES);

        assert_eq!(statements[6].sql, tokens_diffs::INSERT_DIFFS);
        assert_eq!(statements[7].sql, tokens_diffs::INSERT_DIFFS);
        assert_eq!(statements[8].sql, tokens_diffs::INSERT_DIFFS);
        assert_eq!(statements[9].sql, tokens::UPDATE_BALANCES);
        assert_eq!(statements[10].sql, tokens::INSERT_BALANCES);
        assert_eq!(statements[11].sql, tokens::DELETE_ZERO_BALANCES);
    }

    #[test]
    fn check_rollback_statements() -> () {
        let statements = super::prep_rollback(&block_600k());
        assert_eq!(statements.len(), 12);
        assert_eq!(statements[0].sql, tokens::ROLLBACK_DELETE_ZERO_BALANCES);
        assert_eq!(statements[1].sql, tokens::ROLLBACK_BALANCE_UPDATES);
        assert_eq!(statements[2].sql, tokens::DELETE_ZERO_BALANCES);
        assert_eq!(statements[3].sql, tokens_diffs::DELETE_DIFFS);
        assert_eq!(statements[4].sql, tokens_diffs::DELETE_DIFFS);
        assert_eq!(statements[5].sql, tokens_diffs::DELETE_DIFFS);

        assert_eq!(statements[6].sql, erg::ROLLBACK_DELETE_ZERO_BALANCES);
        assert_eq!(statements[7].sql, erg::ROLLBACK_BALANCE_UPDATES);
        assert_eq!(statements[8].sql, erg::DELETE_ZERO_BALANCES);
        assert_eq!(statements[9].sql, erg_diffs::DELETE_DIFFS);
        assert_eq!(statements[10].sql, erg_diffs::DELETE_DIFFS);
        assert_eq!(statements[11].sql, erg_diffs::DELETE_DIFFS);
    }

    #[test]
    fn check_bootstrap_statements() -> () {
        let statements = super::prep_bootstrap(600000);
        assert_eq!(statements.len(), 8);
        // Erg
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

        // Tokens
        assert_eq!(
            statements[4].sql,
            tokens_diffs::bootstrapping::INSERT_DIFFS_AT_HEIGHT
        );
        assert_eq!(statements[5].sql, tokens::UPDATE_BALANCES);
        assert_eq!(statements[6].sql, tokens::INSERT_BALANCES);
        assert_eq!(statements[7].sql, tokens::DELETE_ZERO_BALANCES);
    }
}
