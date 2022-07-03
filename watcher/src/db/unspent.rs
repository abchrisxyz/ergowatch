//! # unspent
//!
//! Maintain set of unspent boxes.

use super::Transaction;
use crate::parsing::BlockData;
use log::info;

pub(super) fn include_block(tx: &mut Transaction, block: &BlockData) -> anyhow::Result<()> {
    // Insert output boxes
    for box_id in extract_outputs(block) {
        tx.execute("insert into usp.boxes (box_id) values ($1);", &[&box_id])
            .unwrap();
    }

    // then delete input boxes
    for box_id in extract_inputs(block) {
        tx.execute("delete from usp.boxes where box_id = $1;", &[&box_id])
            .unwrap();
    }

    Ok(())
}

pub(super) fn rollback_block(tx: &mut Transaction, block: &BlockData) -> anyhow::Result<()> {
    // Insert input boxes
    for box_id in extract_inputs(block) {
        tx.execute("insert into usp.boxes (box_id) values ($1);", &[&box_id])
            .unwrap();
    }

    // then delete output boxes
    for box_id in extract_outputs(block) {
        tx.execute("delete from usp.boxes where box_id = $1;", &[&box_id])
            .unwrap();
    }

    Ok(())
}

pub(super) fn bootstrap(tx: &mut Transaction) -> anyhow::Result<()> {
    if is_bootstrapped(tx) {
        return Ok(());
    }

    info!("Bootstrapping unspent");
    tx.execute("alter table usp.boxes set unlogged;", &[])
        .unwrap();
    // Find all unspent boxes: outputs not used as input
    tx.execute(
        "
        with inputs as (
            select ip.box_id
            from core.inputs ip
            join core.headers hs on hs.id = ip.header_id
        )
        insert into usp.boxes (box_id)
        select op.box_id
        from core.outputs op
        join core.headers hs on hs.id = op.header_id
        left join inputs ip on ip.box_id = op.box_id
        where ip.box_id is null;",
        &[],
    )
    .unwrap();
    tx.execute("alter table usp.boxes set logged;", &[])
        .unwrap();
    set_constraints(tx);
    Ok(())
}

fn is_bootstrapped(tx: &mut Transaction) -> bool {
    let row = tx
        .query_one("select exists(select * from usp.boxes limit 1);", &[])
        .unwrap();
    row.get(0)
}

fn set_constraints(tx: &mut Transaction) {
    let statements = vec!["alter table usp.boxes add primary key (box_id);"];

    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
}

fn extract_outputs<'a>(block: &'a BlockData) -> Vec<&'a str> {
    block
        .transactions
        .iter()
        .flat_map(|tx| tx.outputs.iter().map(|op| op.box_id))
        .collect()
}

fn extract_inputs<'a>(block: &'a BlockData) -> Vec<&'a str> {
    block
        .transactions
        .iter()
        .flat_map(|tx| tx.input_box_ids.iter().map(|&box_id| box_id))
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::parsing::testing::block_600k;

    /*
       Block 600k has x inputs:

       - eb1c4a582ba3e8f9d4af389a19f3bc6fa6759fd33956f9902b34dcd4a1d3842f
       - c739a3294d592377a131840d491bd2b66c27f51ae2c62c66be7bb41b248f321e
       - 6ca2a9d63f2f08663c09d99126ec1be7b65ce2e8f34e283c4d5af78485b47c91
       - 5c029ba7b1c67deedbd68878d02e5d7bb49b54943bc68fb5a30956a7a16224e4

       outputs:
       - 029bc1cb151aaef51c3678d2c74f3e82c9f4d197dd37e7a4eb73612f9da4f1f6
       - 6cb8ffe391838b627cb893c9b2027aa2a03f3a20455dd11e5ac903c7e4179ace
       - aa94183d21f9e8fee38d4f3326d2acf8258dd36e6dff38142fa93e633d01464d
       - 5c029ba7b1c67deedbd68878d02e5d7bb49b54943bc68fb5a30956a7a16224e4
       - 22adc6d1fd18e81da0ab9fa47bc389c5948780c98906c0ea3d812eba4ef17a33
       - 98d0271b7a29d62b672d8dd002e38b8cfbfc8e4055a637422b3e9d59cd6ff86d
    */

    #[test]
    fn outputs() -> () {
        let block = block_600k();
        let box_ids = super::extract_outputs(&block);
        assert_eq!(box_ids.len(), 6);
    }

    #[test]
    fn inputs() -> () {
        let block = block_600k();
        let box_ids = super::extract_inputs(&block);
        assert_eq!(box_ids.len(), 4);
    }
}
