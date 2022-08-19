use crate::parsing::BlockData;
use postgres::types::Type;
use postgres::Transaction;

/// Insert address if new and return address id in any case
const TRY_INSERT_ADDRESS: &str = "
    with test_insert as (
        insert into core.addresses (id, address, spot_height)
        select $1, $2, $3
        -- where not exists (select * from core.addresses where address = $2)
        on conflict do nothing
        returning $1 as id
    )
    select id
    from test_insert
    union 
    select id
    from core.addresses
    where md5(address) = md5($2)
        and address = $2;
    ";

pub(super) fn include(tx: &mut Transaction, block: &BlockData) {
    let statement = tx
        .prepare_typed(
            TRY_INSERT_ADDRESS,
            &[
                Type::INT8, // id
                Type::TEXT, // address
                Type::INT4, // spot_height
            ],
        )
        .unwrap();

    let mut next_id = get_next_address_id(tx);

    for address in extract_addresses(block) {
        let row = tx
            .query_one(&statement, &[&next_id, &address, &block.height])
            .unwrap();
        // Increment id if it was assigned to current address
        let address_id: i64 = row.get(0);
        if address_id == next_id {
            next_id += 1;
        }
    }
}

pub(super) fn include_genesis_boxes(tx: &mut Transaction, boxes: &Vec<crate::parsing::Output>) {
    let block_height = 0i32;
    let mut next_id = get_next_address_id(tx);
    for op in boxes {
        let row = tx
            .query_one(TRY_INSERT_ADDRESS, &[&next_id, &op.address, &block_height])
            .unwrap();
        // Increment id if it was assigned to current address
        let address_id: i64 = row.get(0);
        if address_id == next_id {
            next_id += 1;
        }
    }
}

pub(super) fn rollback(tx: &mut Transaction, block: &BlockData) {
    tx.execute(
        "delete from core.addresses where spot_height = $1;",
        &[&block.height],
    )
    .unwrap();
}

pub(super) fn set_constraints(tx: &mut Transaction) {
    let statements = vec![
        "alter table core.addresses add primary key (id);",
        "alter table core.addresses alter column id set not null;",
        "alter table core.addresses alter column address set not null;",
        "alter table core.addresses alter column spot_height set not null;",
        "create index on core.addresses using brin(spot_height);",
    ];

    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
}

fn extract_addresses<'a>(block: &'a BlockData) -> Vec<&'a String> {
    block
        .transactions
        .iter()
        .flat_map(|tx| tx.outputs.iter().map(|op| &op.address))
        .collect()
}

/// Retrieve next available address id.
///
/// It is the highest id value + 1.
fn get_next_address_id(tx: &mut Transaction) -> i64 {
    let row = tx
        .query_one("select coalesce(max(id), 0) from core.addresses;", &[])
        .unwrap();
    let last_id: i64 = row.get(0);
    last_id + 1
}

#[cfg(test)]
mod tests {
    use super::extract_addresses;
    use crate::parsing::testing::block_with_repeated_addresses;

    #[test]
    fn tets_extract_outputs() -> () {
        let block = block_with_repeated_addresses();
        let addresses = extract_addresses(&block);

        // 1 - "2Z4YBkDsDvQj8BX7xiySFewjitqp2ge9c99jfes2whbtKitZTxdBYqbrVZUvZvKv6aqn9by4kp3LE1c26LCyosFnVnm6b6U1JYvWpYmL2ZnixJbXLjWAWuBThV1D6dLpqZJYQHYDznJCk49g5TUiS4q8khpag2aNmHwREV7JSsypHdHLgJT7MGaw51aJfNubyzSKxZ4AJXFS27EfXwyCLzW1K6GVqwkJtCoPvrcLqmqwacAWJPkmh78nke9H4oT88XmSbRt2n9aWZjosiZCafZ4osUDxmZcc5QVEeTWn8drSraY3eFKe8Mu9MSCcVU"
        // 2 - "88dhgzEuTXaRvR2VKsnXYTGUPh3A9VK8ojeRcpHihcrBu23dnwbB12BbVcJuTcdGfRuSzA8bW25Az6n9"
        // 3 - "jL2aaqw6XU61SZznvcri5VZnx1Gn8hfZWK87JH6PM7o1YMDMZfpH1uoGJSd3gDQabX6AmCZKLyMSBqSoUAo8X7E5oNRV9JgCdLBFjV6i1BEjZLwgGo3RUr4p8zchqrJ1FeGPLf2DidW6F41aeM1zCM64ZjfBqcy8d6fgEnAn53W28GEDQi5W1XCWRjFvgTFuDdAzd6Yj65KGJhdvMSgffP7pELpCtqK5Z4dX9SQKtt8Y4RMBaeEKtKB1pEx1n"
        // 4 - "2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe"
        // 5 - "9h7L7sUHZk43VQC3PHtSp5ujAWcZtYmWATBH746wi75C5XHi68b"
        // 6 - same as 2

        assert_eq!(addresses.len(), 6);
        assert_eq!(addresses[0], "2Z4YBkDsDvQj8BX7xiySFewjitqp2ge9c99jfes2whbtKitZTxdBYqbrVZUvZvKv6aqn9by4kp3LE1c26LCyosFnVnm6b6U1JYvWpYmL2ZnixJbXLjWAWuBThV1D6dLpqZJYQHYDznJCk49g5TUiS4q8khpag2aNmHwREV7JSsypHdHLgJT7MGaw51aJfNubyzSKxZ4AJXFS27EfXwyCLzW1K6GVqwkJtCoPvrcLqmqwacAWJPkmh78nke9H4oT88XmSbRt2n9aWZjosiZCafZ4osUDxmZcc5QVEeTWn8drSraY3eFKe8Mu9MSCcVU");
        assert_eq!(
            addresses[1],
            "88dhgzEuTXaRvR2VKsnXYTGUPh3A9VK8ojeRcpHihcrBu23dnwbB12BbVcJuTcdGfRuSzA8bW25Az6n9"
        );
        assert_eq!(addresses[2], "jL2aaqw6XU61SZznvcri5VZnx1Gn8hfZWK87JH6PM7o1YMDMZfpH1uoGJSd3gDQabX6AmCZKLyMSBqSoUAo8X7E5oNRV9JgCdLBFjV6i1BEjZLwgGo3RUr4p8zchqrJ1FeGPLf2DidW6F41aeM1zCM64ZjfBqcy8d6fgEnAn53W28GEDQi5W1XCWRjFvgTFuDdAzd6Yj65KGJhdvMSgffP7pELpCtqK5Z4dX9SQKtt8Y4RMBaeEKtKB1pEx1n");
        assert_eq!(addresses[3], "2iHkR7CWvD1R4j1yZg5bkeDRQavjAaVPeTDFGGLZduHyfWMuYpmhHocX8GJoaieTx78FntzJbCBVL6rf96ocJoZdmWBL2fci7NqWgAirppPQmZ7fN9V6z13Ay6brPriBKYqLp1bT2Fk4FkFLCfdPpe");
        assert_eq!(
            addresses[4],
            "9h7L7sUHZk43VQC3PHtSp5ujAWcZtYmWATBH746wi75C5XHi68b"
        );
        assert_eq!(
            addresses[5],
            "88dhgzEuTXaRvR2VKsnXYTGUPh3A9VK8ojeRcpHihcrBu23dnwbB12BbVcJuTcdGfRuSzA8bW25Az6n9"
        );
    }
}
