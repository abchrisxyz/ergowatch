use crate::parsing::BlockData;
use postgres::types::Type;
use postgres::Transaction;

struct ErgoTx<'a> {
    pub id: &'a str,
    pub header_id: &'a str,
    pub height: i32,
    pub index: i32,
}

pub(super) fn include(tx: &mut Transaction, block: &BlockData) {
    let sql = "
        insert into core.transactions (id, header_id, height, index)
        values ($1, $2, $3, $4);";

    let statement = tx
        .prepare_typed(
            sql,
            &[
                Type::TEXT, // id
                Type::TEXT, // header_id
                Type::INT4, // height
                Type::INT4, // index
            ],
        )
        .unwrap();

    for t in extract_transactions(block) {
        tx.execute(&statement, &[&t.id, &t.header_id, &t.height, &t.index])
            .unwrap();
    }
}

pub(super) fn rollback(tx: &mut Transaction, block: &BlockData) {
    tx.execute(
        "delete from core.transactions where height = $1",
        &[&block.height],
    )
    .unwrap();
}

pub(super) fn set_constraints(tx: &mut Transaction) {
    let statements = vec![
        "alter table core.transactions add primary key (id);",
        "alter table core.transactions alter column id set not null;",
        "alter table core.transactions alter column header_id set not null;",
        "alter table core.transactions alter column height set not null;",
        "alter table core.transactions alter column index set not null;",
        "alter table core.transactions add foreign key (header_id)
            references core.headers (id) on delete cascade;",
        "create index on core.transactions(height);",
    ];

    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
}

fn extract_transactions<'a>(block: &'a BlockData) -> Vec<ErgoTx<'a>> {
    block
        .transactions
        .iter()
        .map(|tx| ErgoTx {
            id: &tx.id,
            header_id: &block.header_id,
            height: block.height,
            index: tx.index,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::extract_transactions;
    use crate::parsing::testing::block_600k;

    #[test]
    fn extract_data() -> () {
        let block = block_600k();
        let txs = extract_transactions(&block);
        assert_eq!(txs.len(), 3);

        assert_eq!(
            txs[1].id,
            "26dab775e0a6ba4315271db107398b47f6b7ec9c7218165a54938bf58b81c4a8"
        );
        assert_eq!(txs[1].height, 600000);
        assert_eq!(
            txs[1].header_id,
            "5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4"
        );
        assert_eq!(txs[1].index, 1);
    }
}
