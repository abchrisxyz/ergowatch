use super::Cache;
use crate::parsing::BlockData;
use log::info;
use postgres::Transaction;

pub(super) fn include(tx: &mut Transaction, block: &BlockData, cache: &mut Cache) {
    // New value is cached value plus diff
    cache.utxos += extract_utxo_diff(block);

    tx.execute(INSERT_SNAPSHOT, &[&block.height, &cache.utxos])
        .unwrap();
}

pub(super) fn rollback(tx: &mut Transaction, block: &BlockData, cache: &mut Cache) {
    // Old value is cached value minus diff
    cache.utxos -= extract_utxo_diff(block);
    tx.execute(DELETE_SNAPSHOT, &[&block.height]).unwrap();
}

pub fn bootstrap(tx: &mut Transaction) -> anyhow::Result<()> {
    if is_bootstrapped(tx) {
        return Ok(());
    }
    info!("Bootstrapping metrics - utxo's");
    tx.execute("alter table mtr.utxos set unlogged;", &[])?;
    tx.execute(
        "do language plpgsql $$
        declare
            _prev bigint = 0;
            _new bigint;
            _h int;
        begin
            for _h in
                select height
                from core.headers
                order by 1
            loop
                select _prev + (
                    select count(*)
                    from core.outputs op
                    join core.headers hs on hs.id = op.header_id 
                    where hs.height = _h
                ) - (
                    select count(*)
                    from core.inputs op
                    join core.headers hs on hs.id = op.header_id 
                    where hs.height = _h
                ) into _new;
                
                insert into mtr.utxos (height, value) values (_h, _new);
                
                _prev = _new;
            end loop;
        end;
        $$;",
        &[],
    )?;
    tx.execute("alter table mtr.utxos set logged;", &[])?;
    set_constraints(tx);
    Ok(())
}

fn is_bootstrapped(tx: &mut Transaction) -> bool {
    let row = tx
        .query_one("select exists(select * from mtr.utxos limit 1);", &[])
        .unwrap();
    row.get(0)
}

fn set_constraints(tx: &mut Transaction) {
    tx.execute("alter table mtr.utxos add primary key(height);", &[])
        .unwrap();
}

// Add a new snapshot record
const INSERT_SNAPSHOT: &str = "insert into mtr.utxos (height, value) values ($1, $2);";

// Delete snapshot at given height
const DELETE_SNAPSHOT: &str = "delete from mtr.utxos where height = $1;";

// Cache loading
pub const SELECT_LAST_SNAPSHOT_VALUE: &str =
    "select value from mtr.utxos order by height desc limit 1";

/// Change in number of UTxO's
///
/// Number of block ouputs - number of block inputs
fn extract_utxo_diff(block: &BlockData) -> i64 {
    block
        .transactions
        .iter()
        .map(|tx| tx.outputs.len())
        .sum::<usize>() as i64
        - block
            .transactions
            .iter()
            .map(|tx| tx.input_box_ids.len())
            .sum::<usize>() as i64
}

#[cfg(test)]
mod tests {
    use super::extract_utxo_diff;
    use crate::parsing::testing::block_600k;
    use pretty_assertions::assert_eq;

    #[test]
    fn extract() -> () {
        let block = block_600k();
        // UTxO count - block 600k has 4 inputs and 6 outputs
        assert_eq!(extract_utxo_diff(&block), 2);
    }
}
