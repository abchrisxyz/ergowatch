use crate::db::core::assets::BoxAssetRow;
use crate::db::SQLStatement;
use crate::units::Asset;
use crate::units::BlockData;
use itertools::Itertools;

pub(super) fn extract_assets(block: &BlockData) -> Vec<SQLStatement> {
    block
        .transactions
        .iter()
        .flat_map(|tx| {
            tx.outputs.iter().flat_map(|op| {
                op.assets
                    .iter()
                    // Make a copy, so we can group aggregate later on
                    .map(|a| Asset {
                        token_id: a.token_id,
                        amount: a.amount,
                    })
                    // Sum tokens by id
                    .group_by(|a| a.token_id)
                    .into_iter()
                    .map(|(_, group)| group.reduce(|a, b| a + b))
                    .filter(|opt| opt.is_some())
                    .map(|opt| opt.unwrap())
                    .map(|a| {
                        BoxAssetRow {
                            box_id: &op.box_id,
                            token_id: a.token_id,
                            amount: a.amount,
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
    use crate::units::testing::block_600k;
    use crate::units::testing::block_multi_asset_mint;

    #[test]
    fn statements() -> () {
        let statements = extract_assets(&block_600k());
        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0].sql, db::core::assets::INSERT_BOX_ASSET);
    }

    #[test]
    fn asset_aggregation() -> () {
        let statements = extract_assets(&block_multi_asset_mint());
        // 1 asset in first box, 2 in third
        assert_eq!(statements.len(), 1 + 2);
        assert_eq!(statements[0].sql, db::core::assets::INSERT_BOX_ASSET);
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
        assert_eq!(statements[1].args[2], db::SQLArg::BigInt(100));
        assert_eq!(statements[2].args[2], db::SQLArg::BigInt(2));
    }
}
