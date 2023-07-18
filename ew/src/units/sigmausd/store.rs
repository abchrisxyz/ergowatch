use async_trait::async_trait;

use tokio_postgres::NoTls;

use crate::config::PostgresConfig;
use crate::core::types::Head;
use crate::core::types::Height;
use crate::units::Store;
use crate::utils::Schema;

use super::Batch;

mod head;

pub struct SigStore {
    client: tokio_postgres::Client,
    head: Head,
}

#[async_trait]
impl Store for SigStore {
    type B = Batch;
    async fn process(&self, batch: Batch) {
        println!("i: {}", batch.i);
    }
    async fn roll_back(&self, height: Height) {
        todo!()
    }
}

impl SigStore {
    pub async fn new(pgconf: PostgresConfig) -> Self {
        tracing::debug!("initializing new store");
        let (mut client, connection) = tokio_postgres::connect(&pgconf.connection_uri, NoTls)
            .await
            .unwrap();

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        let schema = Schema::new("core", include_str!("store/schema.sql"));
        schema.init(&mut client).await;

        tracing::warn!("Using dummy head");
        let head = Head::initial(); //blocks::last_head(&client).await;
        tracing::debug!("head: {:?}", &head);

        Self { client, head }
    }

    pub(super) fn get_head(&self) -> &Head {
        &self.head
    }
}
