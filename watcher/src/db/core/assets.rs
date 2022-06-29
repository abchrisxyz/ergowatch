use crate::parsing::BlockData;
use postgres::types::Type;
use postgres::Transaction;
use std::collections::HashMap;

struct BoxAsset<'a> {
    pub box_id: &'a str,
    pub token_id: &'a str,
    pub amount: i64,
}

pub(super) fn include(tx: &mut Transaction, block: &BlockData) {
    let assets = extract_assets(block);

    if assets.is_empty() {
        return;
    }

    let sql = "
        insert into core.box_assets (box_id, token_id, amount)
        values ($1, $2, $3);";

    let statement = tx
        .prepare_typed(
            sql,
            &[
                Type::TEXT, // box_id
                Type::TEXT, // token_id
                Type::INT8, // amount
            ],
        )
        .unwrap();

    for ass in assets {
        tx.execute(&statement, &[&ass.box_id, &ass.token_id, &ass.amount])
            .unwrap();
    }
}

pub(super) fn rollback(tx: &mut Transaction, block: &BlockData) {
    tx.execute(
        "
        delete from core.box_assets a
        using core.outputs o
        join core.headers h on h.id = o.header_id
        where h.height = $1 and a.box_id = o.box_id;",
        &[&block.height],
    )
    .unwrap();
}

pub(super) fn set_constraints(tx: &mut Transaction) {
    let statements = vec![
        "alter table core.box_assets add primary key (box_id, token_id);",
        "alter table core.box_assets alter column box_id set not null;",
        "alter table core.box_assets alter column token_id set not null;",
        "alter table core.box_assets alter column amount set not null;",
        "alter table core.box_assets add foreign key (box_id)
            references core.outputs (box_id) on delete cascade;",
        "alter table core.box_assets add check (amount > 0);",
        "create index on core.box_assets (box_id)",
    ];

    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
}

fn extract_assets<'a>(block: &'a BlockData) -> Vec<BoxAsset<'a>> {
    block
        .transactions
        .iter()
        .flat_map(|tx| {
            tx.outputs.iter().flat_map(|op| {
                op.assets
                    .iter()
                    // Sum tokens by id
                    .fold(HashMap::new(), |mut acc, a| {
                        *acc.entry(&a.token_id).or_insert(0) += a.amount;
                        acc
                    })
                    .iter()
                    .map(|(token_id, amount)| BoxAsset {
                        box_id: &op.box_id,
                        token_id: token_id,
                        amount: *amount,
                    })
                    .collect::<Vec<BoxAsset>>()
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::extract_assets;
    use crate::parsing::testing::block_600k;
    use crate::parsing::testing::block_issue27;
    use crate::parsing::testing::block_multi_asset_mint;

    #[test]
    fn assets() -> () {
        let block = block_600k();
        let assets = extract_assets(&block);
        assert_eq!(assets.len(), 1);
        assert_eq!(
            assets[0].box_id,
            "aa94183d21f9e8fee38d4f3326d2acf8258dd36e6dff38142fa93e633d01464d"
        );
        assert_eq!(
            assets[0].token_id,
            "01e6498911823f4d36deaf49a964e883b2c4ae2a4530926f18b9c1411ab2a2c2"
        );
        assert_eq!(assets[0].amount, 1);
    }

    #[test]
    fn asset_aggregation() -> () {
        let block = block_multi_asset_mint();
        let assets = extract_assets(&block);
        // 1 asset in first box, 2 in third
        assert_eq!(assets.len(), 1 + 2);
        // Total amount of aggregated asset is sum of individual records
        assert_eq!(
            assets[0].box_id,
            "e9ad4b744b96abc9244287b21c21720622f57b72d8fb2995c1fe4b4afe63f9d2"
        );
        assert_eq!(
            assets[0].token_id,
            "a342ae8776207b9a7529b93450187a33538ce86b68d11483758debffea667c25"
        );
        assert_eq!(assets[0].amount, 10 + 10);
        // Check amount of other 2 tokens, for good measure
        if assets[1].token_id == "2fc8abf612bc8b36af382e8c10a8e9df6227afdbe508c9b08b0a575fc4937b5e"
        {
            assert_eq!(assets[1].amount, 100);
            assert_eq!(assets[2].amount, 2);
        } else {
            assert_eq!(assets[1].amount, 2);
            assert_eq!(assets[2].amount, 100);
        }
    }

    #[test]
    fn issue_27() -> () {
        let block = block_issue27();
        let assets = extract_assets(&block);
        assert_eq!(assets.len(), 2);
        if assets[0].token_id == "a699d8e6467a9d0bb32d84c135b05dfb0cdddd4fc8e2caa9b9af0aa2666a3a6f"
        {
            assert_eq!(assets[0].amount, 4500);
            assert_eq!(assets[1].amount, 1500);
        } else {
            assert_eq!(assets[0].amount, 1500);
            assert_eq!(assets[1].amount, 4500);
        }
    }
}
