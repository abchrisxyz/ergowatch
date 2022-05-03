use super::sql::assets::BoxAssetRow;
use crate::db::SQLStatement;
use crate::parsing::BlockData;
use std::collections::HashMap;

pub(super) fn extract_assets(block: &BlockData) -> Vec<SQLStatement> {
    block
        .transactions
        .iter()
        .flat_map(|tx| {
            tx.outputs.iter().flat_map(|op| {
                op.assets
                    .iter()
                    // Sum tokens by id
                    .fold(HashMap::new(), |mut acc, a| {
                        *acc.entry(a.token_id).or_insert(0) += a.amount;
                        acc
                    })
                    .iter()
                    .map(|(token_id, amount)| {
                        BoxAssetRow {
                            box_id: &op.box_id,
                            token_id: token_id,
                            amount: *amount,
                        }
                        .to_statement()
                    })
                    .collect::<Vec<SQLStatement>>()
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::extract_assets;
    use crate::db;
    use crate::parsing::testing::block_600k;
    use crate::parsing::testing::block_issue27;
    use crate::parsing::testing::block_multi_asset_mint;

    #[test]
    fn statements() -> () {
        let statements = extract_assets(&block_600k());
        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0].sql, db::core::sql::assets::INSERT_BOX_ASSET);
    }

    #[test]
    fn asset_aggregation() -> () {
        let statements = extract_assets(&block_multi_asset_mint());
        // 1 asset in first box, 2 in third
        assert_eq!(statements.len(), 1 + 2);
        assert_eq!(statements[0].sql, db::core::sql::assets::INSERT_BOX_ASSET);
        // Total amount of aggregated asset is sum of individual records
        let box_id = &statements[0].args[0];
        assert_eq!(
            *box_id,
            db::SQLArg::Text(String::from(
                "e9ad4b744b96abc9244287b21c21720622f57b72d8fb2995c1fe4b4afe63f9d2"
            ))
        );
        let token_id = &statements[0].args[1];
        assert_eq!(
            *token_id,
            db::SQLArg::Text(String::from(
                "a342ae8776207b9a7529b93450187a33538ce86b68d11483758debffea667c25"
            ))
        );
        assert_eq!(statements[0].args[2], db::SQLArg::BigInt(10 + 10));
        // Check amount of other 2 tokens, for good measure
        if statements[1].args[1]
            == db::SQLArg::Text(String::from(
                "2fc8abf612bc8b36af382e8c10a8e9df6227afdbe508c9b08b0a575fc4937b5e",
            ))
        {
            assert_eq!(statements[1].args[2], db::SQLArg::BigInt(100));
            assert_eq!(statements[2].args[2], db::SQLArg::BigInt(2));
        } else {
            assert_eq!(statements[1].args[2], db::SQLArg::BigInt(2));
            assert_eq!(statements[2].args[2], db::SQLArg::BigInt(100));
        }
    }

    #[test]
    fn issue_27() -> () {
        let statements = extract_assets(&block_issue27());
        // 2 assets in first box, 7 in second
        assert_eq!(statements.len(), 0 + 2);
        assert_eq!(statements[0].sql, db::core::sql::assets::INSERT_BOX_ASSET);
        if statements[0].args[1]
            == db::SQLArg::Text(String::from(
                "a699d8e6467a9d0bb32d84c135b05dfb0cdddd4fc8e2caa9b9af0aa2666a3a6f",
            ))
        {
            assert_eq!(statements[0].args[2], db::SQLArg::BigInt(4500));
            assert_eq!(statements[1].args[2], db::SQLArg::BigInt(1500));
        } else {
            assert_eq!(statements[0].args[2], db::SQLArg::BigInt(1500));
            assert_eq!(statements[1].args[2], db::SQLArg::BigInt(4500));
        }
    }
}
