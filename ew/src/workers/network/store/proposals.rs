use crate::core::types::Height;
use tokio_postgres::GenericClient;
use tokio_postgres::Transaction;

use super::super::types::ProposalRecord;

/// Insert a record
pub(super) async fn insert(pgtx: &Transaction<'_>, record: &ProposalRecord) {
    tracing::trace!("insert {record:?}");
    let stmt = "
        insert into network.proposals (
            epoch,
            height,
            miner_address_id,
            slots,
            tallies
            
        )
        values ($1, $2, $3, array[$4, $5, $6]::smallint[], array[$7, $8, $9]::smallint[]);";
    pgtx.execute(
        stmt,
        &[
            &record.epoch,
            &record.height,
            &record.miner_address_id,
            &record.slots[0],
            &record.slots[1],
            &record.slots[2],
            &record.tally[0],
            &record.tally[1],
            &record.tally[2],
        ],
    )
    .await
    .unwrap();
}

/// Update proposal tally
pub(super) async fn update(pgtx: &Transaction<'_>, record: &ProposalRecord) {
    tracing::trace!("update {record:?}");
    let stmt = "
        update network.proposals
        set tallies[1] = $2,
            tallies[2] = $3,
            tallies[3] = $4
        where epoch = $1;";
    pgtx.execute(
        stmt,
        &[
            &record.epoch,
            &record.tally[0],
            &record.tally[1],
            &record.tally[2],
        ],
    )
    .await
    .unwrap();
}

/// Delete a proposal created at given height
pub(super) async fn delete_at(pgtx: &Transaction<'_>, height: Height) {
    tracing::trace!("delete_at {height}");
    let stmt = "delete from network.proposals where height = $1;";
    pgtx.execute(stmt, &[&height]).await.unwrap();
}

/// Insert a record
pub(super) async fn get_last(client: &impl GenericClient) -> Option<ProposalRecord> {
    tracing::trace!("get_last");
    let stmt = "
        select epoch
            , height
            , miner_address_id
            , slots[1]
            , slots[2]
            , slots[3]
            , tallies[1]
            , tallies[2]
            , tallies[3]
        from network.proposals
        order by height desc
        limit 1;";
    client.query_opt(stmt, &[]).await.unwrap().and_then(|row| {
        Some(ProposalRecord {
            epoch: row.get(0),
            height: row.get(1),
            miner_address_id: row.get(2),
            slots: [row.get(3), row.get(4), row.get(5)],
            tally: [row.get(6), row.get(7), row.get(8)],
        })
    })
}
