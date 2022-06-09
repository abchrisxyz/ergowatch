use crate::parsing::BlockData;
use postgres::types::Type;
use postgres::Transaction;

struct Input<'a> {
    pub box_id: &'a str,
    pub tx_id: &'a str,
    pub header_id: &'a str,
    pub index: i32,
}

pub(super) fn include(tx: &mut Transaction, block: &BlockData) {
    let sql = "
        insert into core.inputs (box_id, tx_id, header_id, index)
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

    for ip in extract_inputs(block) {
        tx.execute(
            &statement,
            &[&ip.box_id, &ip.tx_id, &ip.header_id, &ip.index],
        )
        .unwrap();
    }
}

pub(super) fn rollback(tx: &mut Transaction, block: &BlockData) {
    tx.execute(
        "
        delete from core.inputs i
        using core.headers h
        where h.height = $1 and i.header_id = h.id;",
        &[&block.height],
    )
    .unwrap();
}

pub(super) fn set_constraints(tx: &mut Transaction) {
    let statements = vec![
        "alter table core.inputs add primary key (box_id);",
        "alter table core.inputs alter column box_id set not null;",
        "alter table core.inputs alter column tx_id set not null;",
        "alter table core.inputs alter column header_id set not null;",
        "alter table core.inputs alter column index set not null;",
        "alter table core.inputs add foreign key (tx_id)
            references core.transactions (id) on delete cascade;",
        "alter table core.inputs add foreign key (header_id)
            references core.headers (id) on delete cascade;",
        "create index on core.inputs(tx_id);",
        "create index on core.inputs(header_id);",
        "create index on core.inputs(index);",
    ];

    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
}

fn extract_inputs<'a>(block: &'a BlockData) -> Vec<Input<'a>> {
    block
        .transactions
        .iter()
        .flat_map(|tx| {
            tx.input_box_ids.iter().enumerate().map(|(ix, id)| Input {
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
    use super::extract_inputs;
    use crate::parsing::testing::block_600k;

    #[test]
    fn extract_data() -> () {
        let block = block_600k();
        let ips = extract_inputs(&block);
        assert_eq!(ips.len(), 4);
        assert_eq!(
            ips[1].box_id,
            "c739a3294d592377a131840d491bd2b66c27f51ae2c62c66be7bb41b248f321e"
        );
        assert_eq!(
            ips[1].tx_id,
            "26dab775e0a6ba4315271db107398b47f6b7ec9c7218165a54938bf58b81c4a8"
        );
        assert_eq!(
            ips[1].header_id,
            "5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4"
        );
        assert_eq!(ips[1].index, 0);
    }
}
