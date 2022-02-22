use super::super::Output;
use super::BlockData;
use crate::db::core::registers::BoxRegisterRow;
use crate::db::SQLStatement;

pub(super) fn extract_additional_registers(block: &BlockData) -> Vec<SQLStatement> {
    block
        .transactions
        .iter()
        .flat_map(|tx| tx.outputs.iter().flat_map(|op| extract_from_output(op)))
        .collect()
}

pub(super) fn extract_from_output(op: &Output) -> Vec<SQLStatement> {
    op.additional_registers
        .iter()
        .filter(|r| r.is_some())
        .map(|r| r.as_ref().unwrap())
        .map(|r| {
            BoxRegisterRow {
                id: r.id,
                box_id: &op.box_id,
                stype: &r.stype,
                serialized_value: &r.serialized_value,
                rendered_value: &r.rendered_value,
            }
            .to_statement()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::extract_additional_registers;
    use crate::db;
    use crate::units::testing::block_600k;
    use pretty_assertions::assert_eq;

    #[test]
    fn register_statements() -> () {
        let statements = extract_additional_registers(&block_600k());
        assert_eq!(statements.len(), 3);
        assert_eq!(statements[0].sql, db::core::registers::INSERT_BOX_REGISTER);
        assert_eq!(statements[1].sql, db::core::registers::INSERT_BOX_REGISTER);
        assert_eq!(statements[2].sql, db::core::registers::INSERT_BOX_REGISTER);
    }

    #[test]
    fn statement_args() -> () {
        // Looking at statement args for R4 of first output of second tx in block 600k
        let statements = extract_additional_registers(&block_600k());
        let args = &statements[0].args;
        // Register id
        assert_eq!(args[0], db::SQLArg::SmallInt(4));
        // Box id
        assert_eq!(
            args[1],
            db::SQLArg::Text(String::from(
                "aa94183d21f9e8fee38d4f3326d2acf8258dd36e6dff38142fa93e633d01464d"
            ))
        );
        // Value type
        assert_eq!(args[2], db::SQLArg::Text(String::from("SGroupElement")));
        // Serialized value
        assert_eq!(
            args[3],
            db::SQLArg::Text(String::from(
                "0703553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8"
            ))
        );
        // Rendered value
        assert_eq!(
            args[4],
            db::SQLArg::Text(String::from(
                "03553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8"
            ))
        );
    }
}
