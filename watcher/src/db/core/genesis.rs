use crate::db::balances;
use crate::db::balances::erg_diffs::ErgDiffQuery;
use crate::db::core::sql::header::HeaderRow;
use crate::db::core::sql::outputs::OutputRow;
use crate::db::core::sql::transaction::TransactionRow;
use crate::db::unspent;
use crate::db::SQLStatement;
use crate::parsing::Output;

const GENESIS_TIMESTAMP: i64 = 1561978800000;

/// Helper function to include genesis boxes in db.
///
/// Includes dummy header and tx to satisfy FK's.
/// Also affects depending schemas.
pub fn prep(node_boxes: Vec<crate::node::models::Output>) -> Vec<SQLStatement> {
    let mut statements: Vec<SQLStatement> = vec![
        HeaderRow {
            height: 0,
            id: "0000000000000000000000000000000000000000000000000000000000000000",
            parent_id: "genesis",
            timestamp: GENESIS_TIMESTAMP,
        }
        .to_statement(),
        TransactionRow {
            id: "0000000000000000000000000000000000000000000000000000000000000000",
            header_id: "0000000000000000000000000000000000000000000000000000000000000000",
            height: 0,
            index: 0,
        }
        .to_statement(),
    ];
    // Outputs
    statements.append(
        &mut node_boxes
            .iter()
            .map(|node_box| {
                let output = Output::from_node_output(node_box);
                OutputRow {
                    box_id: &output.box_id,
                    tx_id: &node_box.transaction_id,
                    header_id: "0000000000000000000000000000000000000000000000000000000000000000",
                    creation_height: output.creation_height,
                    address: &output.address,
                    index: output.index,
                    value: output.value,
                }
                .to_statement()
            })
            .collect(),
    );
    // Additional registers
    statements.append(
        &mut node_boxes
            .iter()
            .flat_map(|node_box| {
                let output = Output::from_node_output(node_box);
                super::additional_registers::extract_from_output(&output)
            })
            .collect(),
    );
    // TODO: refactor genesis out of core unit since it affects multiple schemas
    // TODO: test db state after genesis boxes inclusion (in testbench)
    // Unspent boxes
    statements.append(
        &mut node_boxes
            .iter()
            .map(|node_box| {
                let output = Output::from_node_output(node_box);
                unspent::usp::insert_new_box_statement(&output.box_id)
            })
            .collect(),
    );
    // Erg balance diffs
    statements.push(
        ErgDiffQuery {
            tx_id: "0000000000000000000000000000000000000000000000000000000000000000",
        }
        .to_statement(),
    );
    // Erg balance
    statements.push(balances::erg::insert_statement(0));

    statements
}

#[cfg(test)]
mod tests {
    use super::prep;
    use crate::db;
    use crate::db::core::sql;
    use crate::node::models::testing::genesis_boxes;

    #[test]
    fn statements() -> () {
        let statements: Vec<db::SQLStatement> = prep(genesis_boxes());
        assert_eq!(statements.len(), 16);
        // Core
        assert_eq!(statements[0].sql, sql::header::INSERT_HEADER);
        assert_eq!(statements[1].sql, sql::transaction::INSERT_TRANSACTION);
        assert_eq!(statements[2].sql, sql::outputs::INSERT_OUTPUT);
        assert_eq!(statements[3].sql, sql::outputs::INSERT_OUTPUT);
        assert_eq!(statements[4].sql, sql::outputs::INSERT_OUTPUT);
        assert_eq!(statements[5].sql, sql::registers::INSERT_BOX_REGISTER);
        assert_eq!(statements[6].sql, sql::registers::INSERT_BOX_REGISTER);
        assert_eq!(statements[7].sql, sql::registers::INSERT_BOX_REGISTER);
        assert_eq!(statements[8].sql, sql::registers::INSERT_BOX_REGISTER);
        assert_eq!(statements[9].sql, sql::registers::INSERT_BOX_REGISTER);
        assert_eq!(statements[10].sql, sql::registers::INSERT_BOX_REGISTER);
        // Others
        assert_eq!(statements[11].sql, db::unspent::usp::INSERT_NEW_BOX);
        assert_eq!(statements[12].sql, db::unspent::usp::INSERT_NEW_BOX);
        assert_eq!(statements[13].sql, db::unspent::usp::INSERT_NEW_BOX);
        assert_eq!(statements[14].sql, db::balances::erg_diffs::INSERT_DIFFS);
        assert_eq!(statements[15].sql, db::balances::erg::INSERT_BALANCES);

        // Timestamp of genesis header
        let statement = &statements[0];
        assert_eq!(
            statement.args[3],
            db::SQLArg::BigInt(super::GENESIS_TIMESTAMP)
        );

        // First box statement
        let statement = &statements[2];
        // Box ID
        assert_eq!(
            statement.args[0],
            db::SQLArg::Text(
                "b69575e11c5c43400bfead5976ee0d6245a1168396b2e2a4f384691f275d501c".to_owned()
            )
        );
        // Tx ID
        assert_eq!(
            statement.args[1],
            db::SQLArg::Text(
                "0000000000000000000000000000000000000000000000000000000000000000".to_owned()
            )
        );
        // Header ID
        assert_eq!(
            statement.args[2],
            db::SQLArg::Text(
                "0000000000000000000000000000000000000000000000000000000000000000".to_owned()
            )
        );
        // Creation height
        assert_eq!(statement.args[3], db::SQLArg::Integer(0));
        // Address (should be coinbase address)
        assert_eq!(statement.args[4], db::SQLArg::Text("2Z4YBkDsDvQj8BX7xiySFewjitqp2ge9c99jfes2whbtKitZTxdBYqbrVZUvZvKv6aqn9by4kp3LE1c26LCyosFnVnm6b6U1JYvWpYmL2ZnixJbXLjWAWuBThV1D6dLpqZJYQHYDznJCk49g5TUiS4q8khpag2aNmHwREV7JSsypHdHLgJT7MGaw51aJfNubyzSKxZ4AJXFS27EfXwyCLzW1K6GVqwkJtCoPvrcLqmqwacAWJPkmh78nke9H4oT88XmSbRt2n9aWZjosiZCafZ4osUDxmZcc5QVEeTWn8drSraY3eFKe8Mu9MSCcVU".to_owned()));
        // Index
        assert_eq!(statement.args[5], db::SQLArg::Integer(0));
        // Value
        assert_eq!(statement.args[6], db::SQLArg::BigInt(93409132500000000));
    }
}
