use crate::parsing::BlockData;
use postgres::types::Type;
use postgres::Transaction;

struct DataInput<'a> {
    pub box_id: &'a str,
    pub tx_id: &'a str,
    pub header_id: &'a str,
    pub index: i32,
}

pub(super) fn include(tx: &mut Transaction, block: &BlockData) {
    let sql = "
        insert into core.data_inputs (box_id, tx_id, header_id, index)
        values ($1, $2, $3, $4);";

    let statement = tx
        .prepare_typed(
            sql,
            &[
                Type::TEXT, // box_id
                Type::TEXT, // tx_id
                Type::TEXT, // header_id
                Type::INT4, // index
            ],
        )
        .unwrap();

    for di in extract_data_inputs(block) {
        tx.execute(
            &statement,
            &[&di.box_id, &di.tx_id, &di.header_id, &di.index],
        )
        .unwrap();
    }
}

pub(super) fn set_constraints(tx: &mut Transaction) {
    let statements = vec![
        "alter table core.data_inputs add primary key (box_id, tx_id);",
        "alter table core.data_inputs alter column header_id set not null;",
        "alter table core.data_inputs add foreign key (tx_id)
            references core.transactions (id) on delete cascade;",
        "alter table core.data_inputs add foreign key (header_id)
            references core.headers (id) on delete cascade;",
        "alter table core.data_inputs add foreign key (box_id)
            references core.outputs (box_id) on delete cascade;",
        "create index on core.data_inputs(tx_id);",
        "create index on core.data_inputs(header_id);",
    ];

    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
}

fn extract_data_inputs<'a>(block: &'a BlockData) -> Vec<DataInput<'a>> {
    block
        .transactions
        .iter()
        .flat_map(|tx| {
            tx.data_input_box_ids
                .iter()
                .enumerate()
                .map(|(ix, id)| DataInput {
                    box_id: &id,
                    tx_id: &tx.id,
                    header_id: &block.header_id,
                    index: ix as i32,
                })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::extract_data_inputs;
    use crate::parsing::testing::block_600k;

    #[test]
    fn statements() -> () {
        let block = block_600k();
        let dis = extract_data_inputs(&block);
        assert_eq!(dis.len(), 1);
        assert_eq!(
            dis[0].box_id,
            "98479c7d306cccbd653301102762d79515fa04c6f6b35056aaf2bd77a7299bb8"
        );
        assert_eq!(
            dis[0].tx_id,
            "26dab775e0a6ba4315271db107398b47f6b7ec9c7218165a54938bf58b81c4a8"
        );
        assert_eq!(
            dis[0].header_id,
            "5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4"
        );
        assert_eq!(dis[0].index, 0);
    }
}
