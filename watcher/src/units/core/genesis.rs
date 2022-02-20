use crate::db::core::header::HeaderRow;
use crate::db::core::outputs::OutputRow;
use crate::db::core::transaction::TransactionRow;
use crate::db::SQLStatement;

/// Helper function to include genesis boxes in db.
///
/// Includes dummy header and tx to satisfy FK's.
pub fn prep(node_boxes: Vec<crate::node::models::Output>) -> Vec<SQLStatement> {
    let mut statements: Vec<SQLStatement> = vec![
        HeaderRow {
            height: 0,
            id: "0000000000000000000000000000000000000000000000000000000000000000",
            parent_id: "genesis",
            timestamp: 0,
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
    statements.append(
        &mut node_boxes
            .iter()
            .map(|node_box| {
                let output = super::super::Output::from_node_output(node_box);
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
    statements
}

#[cfg(test)]
mod tests {
    use super::prep;
    use crate::db;
    use crate::node::models::testing::genesis_boxes;

    #[test]
    fn statement() -> () {
        let statements: Vec<db::SQLStatement> = prep(genesis_boxes());
        assert_eq!(statements.len(), 5);
        assert_eq!(statements[0].sql, db::core::header::INSERT_HEADER);
        assert_eq!(statements[1].sql, db::core::transaction::INSERT_TRANSACTION);
        assert_eq!(statements[2].sql, db::core::outputs::INSERT_OUTPUT);
        assert_eq!(statements[3].sql, db::core::outputs::INSERT_OUTPUT);
        assert_eq!(statements[4].sql, db::core::outputs::INSERT_OUTPUT);

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
