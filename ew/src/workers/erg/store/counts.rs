use tokio_postgres::Client;

use super::super::types::AddressCounts;
use super::super::types::AddressCountsRecord;

pub(super) async fn get_last(client: &Client) -> AddressCounts {
    AddressCounts {
        p2pk: get_last_p2pk(client).await,
        contracts: get_last_contracts(client).await,
        miners: get_last_miners(client).await,
    }
}

async fn get_last_p2pk(client: &Client) -> AddressCountsRecord {
    todo!()
}

async fn get_last_contracts(client: &Client) -> AddressCountsRecord {
    todo!()
}

async fn get_last_miners(client: &Client) -> AddressCountsRecord {
    todo!()
}
