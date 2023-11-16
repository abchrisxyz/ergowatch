use std::collections::HashMap;
use tokio_postgres::GenericClient;
use tokio_postgres::Transaction;

use super::super::types::DepositAddressConflictRecord;
use crate::core::types::AddressID;
use crate::core::types::Height;
use crate::workers::exchanges::types::ExchangeID;

pub(super) async fn insert(pgtx: &Transaction<'_>, record: &DepositAddressConflictRecord) {
    tracing::trace!("insert {record:?}");
    let sql = "
        insert into exchanges.deposit_addresses_excluded (
            address_id,
            first_cex_id,
            deposit_spot_height,
            conflict_spot_height
        )
        values ($1, $2, $3, $4);
    ";
    pgtx.execute(
        sql,
        &[
            &record.address_id,
            &record.first_cex_id,
            &record.deposit_spot_height,
            &record.conflict_spot_height,
        ],
    )
    .await
    .unwrap();
}

pub(super) async fn map_all(client: &impl GenericClient) -> HashMap<AddressID, Option<ExchangeID>> {
    tracing::trace!("get_all");
    let qry = "
        select address_id
            , first_cex_id
        from exchanges.deposit_addresses_excluded;
    ";
    HashMap::from_iter(
        client
            .query(qry, &[])
            .await
            .unwrap()
            .into_iter()
            .map(|r| (r.get(0), r.get(1))),
    )
}

/// Returns records for which a conflict was spotted at given `h`
pub(super) async fn get_conflicted_at(
    client: &impl GenericClient,
    height: Height,
) -> Vec<DepositAddressConflictRecord> {
    tracing::trace!("get_conflicted_at {height}");
    let qry = "
        select address_id
            , first_cex_id
            , deposit_spot_height
            , conflict_spot_height
        from exchanges.deposit_addresses_excluded
        where conflict_spot_height = $1;
    ";
    client
        .query(qry, &[&height])
        .await
        .unwrap()
        .iter()
        .map(|r| DepositAddressConflictRecord {
            address_id: r.get(0),
            first_cex_id: r.get(1),
            deposit_spot_height: r.get(2),
            conflict_spot_height: r.get(3),
        })
        .collect()
}

/// Deletes addresses for which a conflict was spotted at given `h`
pub(super) async fn delete_conflicted_at(pgtx: &Transaction<'_>, height: Height) {
    tracing::trace!("delete_conflicted_at {height}");
    let sql = "
        delete from exchanges.deposit_addresses_excluded
        where conflict_spot_height = $1;
    ";
    pgtx.query(sql, &[&height]).await.unwrap();
}
