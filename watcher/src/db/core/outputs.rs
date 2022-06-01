use crate::parsing::BlockData;
use postgres::types::Type;
use postgres::Transaction;

const INSERT_OUTPUT: &str = "
    insert into core.outputs (
        box_id,
        tx_id,
        header_id,
        creation_height,
        address,
        index,
        value
    )
    values ($1, $2, $3, $4, $5, $6, $7);";

pub(super) struct Output<'a> {
    pub box_id: &'a str,
    pub tx_id: &'a str,
    pub header_id: &'a str,
    pub creation_height: i32,
    pub address: &'a str,
    pub index: i32,
    pub value: i64,
}

pub(super) fn include(tx: &mut Transaction, block: &BlockData) {
    let statement = tx
        .prepare_typed(
            INSERT_OUTPUT,
            &[
                Type::TEXT, // box_id
                Type::TEXT, // tx_id
                Type::TEXT, // header_id
                Type::INT4, // creation_height
                Type::TEXT, // address
                Type::INT4, // index
                Type::INT8, // value
            ],
        )
        .unwrap();

    for op in extract_outputs(block) {
        tx.execute(
            &statement,
            &[
                &op.box_id,
                &op.tx_id,
                &op.header_id,
                &op.creation_height,
                &op.address,
                &op.index,
                &op.value,
            ],
        )
        .unwrap();
    }
}

pub(super) fn include_genesis_boxes(
    tx: &mut Transaction,
    boxes: &Vec<crate::parsing::Output>,
    header_id: &str,
    tx_id: &str,
) {
    for op in boxes {
        tx.execute(
            INSERT_OUTPUT,
            &[
                &op.box_id,
                &tx_id,
                &header_id,
                &op.creation_height,
                &op.address,
                &op.index,
                &op.value,
            ],
        )
        .unwrap();
    }
}

pub(super) fn set_constraints(tx: &mut Transaction) {
    let statements = vec![
        "alter table core.outputs add primary key (box_id);",
        "alter table core.outputs alter column box_id set not null;",
        "alter table core.outputs alter column tx_id set not null;",
        "alter table core.outputs alter column header_id set not null;",
        "alter table core.outputs alter column creation_height set not null;",
        "alter table core.outputs alter column address set not null;",
        "alter table core.outputs alter column index set not null;",
        "alter table core.outputs alter column value set not null;",
        "alter table core.outputs add foreign key (tx_id)
            references core.transactions (id) on delete cascade;",
        "alter table core.outputs add foreign key (header_id)
            references core.headers (id) on delete cascade;",
        "create index on core.outputs(tx_id);",
        "create index on core.outputs(header_id);",
        "create index on core.outputs(address);",
        "create index on core.outputs(index);",
    ];

    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
}

fn extract_outputs<'a>(block: &'a BlockData) -> Vec<Output<'a>> {
    block
        .transactions
        .iter()
        .flat_map(|tx| {
            tx.outputs.iter().map(|op| Output {
                box_id: &op.box_id,
                tx_id: &tx.id,
                header_id: &block.header_id,
                creation_height: op.creation_height,
                address: &op.address,
                index: op.index,
                value: op.value,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::extract_outputs;
    use crate::parsing::testing::block_600k;

    #[test]
    fn tets_extract_outputs() -> () {
        let block = block_600k();
        let outputs = extract_outputs(&block);
        assert_eq!(outputs.len(), 6);

        // Check data for 3rd output of 2nd tx
        let op = &outputs[4];
        assert_eq!(
            op.box_id,
            "22adc6d1fd18e81da0ab9fa47bc389c5948780c98906c0ea3d812eba4ef17a33"
        );
        assert_eq!(
            op.tx_id,
            "26dab775e0a6ba4315271db107398b47f6b7ec9c7218165a54938bf58b81c4a8"
        );
        assert_eq!(
            op.header_id,
            "5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4"
        );
        assert_eq!(op.creation_height, 599998);
        assert_eq!(
            op.address,
            "9h7L7sUHZk43VQC3PHtSp5ujAWcZtYmWATBH746wi75C5XHi68b"
        );
        assert_eq!(op.index, 2);
        assert_eq!(op.value, 2784172525);
    }
}
