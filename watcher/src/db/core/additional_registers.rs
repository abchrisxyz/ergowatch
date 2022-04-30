use crate::parsing::BlockData;
use crate::parsing::Output;
use postgres::types::Type;
use postgres::Transaction;

const INSERT_REGISTER: &str = "
    insert into core.box_registers (
        id,
        box_id,
        value_type,
        serialized_value,
        rendered_value
    )
    values ($1, $2, $3, $4, $5);";

struct BoxRegister<'a> {
    pub id: i16,
    pub box_id: &'a str,
    pub stype: &'a str,
    pub serialized_value: &'a str,
    pub rendered_value: &'a str,
}

pub(super) fn include(tx: &mut Transaction, block: &BlockData) {
    let statement = tx
        .prepare_typed(
            INSERT_REGISTER,
            &[
                Type::INT2, // id
                Type::TEXT, // box_id
                Type::TEXT, // value_type
                Type::TEXT, // serialized_value
                Type::TEXT, // rendered_value
            ],
        )
        .unwrap();

    for reg in extract_additional_registers(block) {
        tx.execute(
            &statement,
            &[
                &reg.id,
                &reg.box_id,
                &reg.stype,
                &reg.serialized_value,
                &reg.rendered_value,
            ],
        )
        .unwrap();
    }
}

pub(super) fn include_genesis_boxes(tx: &mut Transaction, boxes: &Vec<crate::parsing::Output>) {
    for output in boxes {
        for reg in extract_from_output(&output) {
            tx.execute(
                INSERT_REGISTER,
                &[
                    &reg.id,
                    &reg.box_id,
                    &reg.stype,
                    &reg.serialized_value,
                    &reg.rendered_value,
                ],
            )
            .unwrap();
        }
    }
}

pub(super) fn set_constraints(tx: &mut Transaction) {
    let statements = vec![
        "alter table core.box_registers add primary key (id, box_id);",
        "alter table core.box_registers add foreign key (box_id)
            references core.outputs (box_id) on delete cascade;",
        "alter table core.box_registers add check (id >= 4 and id <= 9);",
    ];

    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
}

fn extract_additional_registers<'a>(block: &'a BlockData) -> Vec<BoxRegister<'a>> {
    block
        .transactions
        .iter()
        .flat_map(|tx| tx.outputs.iter().flat_map(|op| extract_from_output(op)))
        .collect()
}

fn extract_from_output<'a>(op: &'a Output) -> Vec<BoxRegister<'a>> {
    op.additional_registers
        .iter()
        .filter(|r| r.is_some())
        .map(|r| r.as_ref().unwrap())
        .map(|r| BoxRegister {
            id: r.id,
            box_id: &op.box_id,
            stype: &r.stype,
            serialized_value: &r.serialized_value,
            rendered_value: &r.rendered_value,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::extract_additional_registers;
    use crate::parsing::testing::block_600k;
    use pretty_assertions::assert_eq;

    #[test]
    fn extract_data() -> () {
        let block = block_600k();
        let registers = extract_additional_registers(&block);
        assert_eq!(registers.len(), 3);

        // R4 of first output of second tx in block 600k
        let r = &registers[0];
        assert_eq!(r.id, 4);
        assert_eq!(
            r.box_id,
            "aa94183d21f9e8fee38d4f3326d2acf8258dd36e6dff38142fa93e633d01464d"
        );
        assert_eq!(r.stype, "SGroupElement");
        assert_eq!(
            r.serialized_value,
            "0703553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8"
        );
        assert_eq!(
            r.rendered_value,
            "03553448c194fdd843c87d080f5e8ed983f5bb2807b13b45a9683bba8c7bfb5ae8"
        );
    }
}
