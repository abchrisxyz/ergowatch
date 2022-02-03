//! # core
//!
//! Process blocks into core tables data.

mod additional_registers;
use additional_registers::extract_additional_registers;

mod assets;
use assets::extract_assets;

mod data_inputs;
use data_inputs::extract_data_inputs;

mod headers;
use headers::extract_header;
use headers::rollback_header;

mod inputs;
use inputs::extract_inputs;

mod outputs;
use outputs::extract_outputs;

mod tokens;
use tokens::extract_new_tokens;

mod transactions;
use transactions::extract_transactions;

use super::BlockData;
use crate::db::SQLStatement;

pub struct CoreUnit;

impl CoreUnit {
    pub fn prep(&self, block: &BlockData) -> Vec<SQLStatement> {
        let mut statements: Vec<SQLStatement> = vec![];
        statements.push(extract_header(&block));
        statements.append(&mut extract_transactions(&block));
        statements.append(&mut extract_outputs(&block));
        statements.append(&mut extract_inputs(&block));
        statements.append(&mut extract_data_inputs(&block));
        statements.append(&mut extract_additional_registers(&block));
        statements.append(&mut extract_new_tokens(&block));
        statements.append(&mut extract_assets(&block));
        statements
    }

    pub fn prep_rollback(&self, block: &BlockData) -> Vec<SQLStatement> {
        // Enough to delete header only.
        // Foreigh keys will propagate delete to other core tables.
        vec![rollback_header(&block)]
    }
}

#[cfg(test)]
mod tests {
    use super::CoreUnit;
    use crate::db;
    use crate::units::testing::block_600k;

    #[test]
    fn statements_order() -> () {
        let statements = CoreUnit.prep(&block_600k());
        // 1 header + 3 transactions + 6 outputs + 4 inputs + 1 data input + 3 registers + 1 asset
        assert_eq!(statements.len(), 19);
        assert_eq!(statements[0].sql, db::core::header::INSERT_HEADER);
        assert_eq!(statements[1].sql, db::core::transaction::INSERT_TRANSACTION);
        assert_eq!(statements[2].sql, db::core::transaction::INSERT_TRANSACTION);
        assert_eq!(statements[3].sql, db::core::transaction::INSERT_TRANSACTION);
        assert_eq!(statements[4].sql, db::core::outputs::INSERT_OUTPUT);
        assert_eq!(statements[5].sql, db::core::outputs::INSERT_OUTPUT);
        assert_eq!(statements[6].sql, db::core::outputs::INSERT_OUTPUT);
        assert_eq!(statements[7].sql, db::core::outputs::INSERT_OUTPUT);
        assert_eq!(statements[8].sql, db::core::outputs::INSERT_OUTPUT);
        assert_eq!(statements[9].sql, db::core::outputs::INSERT_OUTPUT);
        assert_eq!(statements[10].sql, db::core::inputs::INSERT_INPUT);
        assert_eq!(statements[11].sql, db::core::inputs::INSERT_INPUT);
        assert_eq!(statements[12].sql, db::core::inputs::INSERT_INPUT);
        assert_eq!(statements[13].sql, db::core::inputs::INSERT_INPUT);
        assert_eq!(statements[14].sql, db::core::data_inputs::INSERT_DATA_INPUT);
        assert_eq!(statements[15].sql, db::core::registers::INSERT_BOX_REGISTER);
        assert_eq!(statements[16].sql, db::core::registers::INSERT_BOX_REGISTER);
        assert_eq!(statements[17].sql, db::core::registers::INSERT_BOX_REGISTER);
        assert_eq!(statements[18].sql, db::core::assets::INSERT_BOX_ASSET);
    }
}
