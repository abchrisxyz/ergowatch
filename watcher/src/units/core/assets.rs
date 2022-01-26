use super::BlockData;
use crate::db::core::assets::BoxAssetRow;
use crate::db::SQLStatement;

pub(super) fn extract_assets(block: &BlockData) -> Vec<SQLStatement> {
    block
        .transactions
        .iter()
        .flat_map(|tx| {
            tx.outputs.iter().flat_map(|op| {
                op.assets.iter().map(|a| {
                    BoxAssetRow {
                        box_id: &op.box_id,
                        token_id: a.token_id,
                        amount: a.amount,
                    }
                    .to_statement()
                })
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::extract_assets;
    use crate::db;
    use crate::units::testing::block_600k;

    #[test]
    fn statements() -> () {
        let statements = extract_assets(&block_600k());
        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0].sql, db::core::assets::INSERT_BOX_ASSET);
    }
}
