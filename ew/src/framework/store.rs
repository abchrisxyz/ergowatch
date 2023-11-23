use async_trait::async_trait;
use tokio_postgres::Client;
use tokio_postgres::NoTls;
use tokio_postgres::Transaction;

use super::StampedData;
use crate::config::PostgresConfig;
use crate::core::types::Header;
use crate::core::types::Height;

impl<D> From<&StampedData<D>> for Header {
    fn from(value: &StampedData<D>) -> Self {
        Self {
            height: value.height,
            timestamp: value.timestamp,
            header_id: value.header_id.clone(),
            parent_id: value.parent_id.clone(),
        }
    }
}

#[async_trait]
pub trait BatchStore {
    type B;

    async fn new() -> Self;

    async fn persist(&mut self, pgtx: &Transaction<'_>, stamped_batch: &StampedData<Self::B>);

    async fn roll_back(&mut self, pgtx: &Transaction<'_>, header: &Header);
}

pub struct PgStore<B: BatchStore> {
    client: Client,
    schema: &'static str,
    worker_id: &'static str,
    header: Header,
    batch_store: B,
}

impl<B: BatchStore> PgStore<B> {
    pub fn get_client(&self) -> &Client {
        &self.client
    }

    pub async fn new(pgconf: &PostgresConfig, schema: &Schema, worker_id: &'static str) -> Self {
        tracing::debug!("initializing store {}:{worker_id}", schema.name);

        // init client
        let (mut client, connection) = tokio_postgres::connect(&pgconf.connection_uri, NoTls)
            .await
            .unwrap();

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        // Create schema if needed
        schema.init(&mut client).await;

        // Init header
        let header = match headers::get(&client, schema.name, worker_id).await {
            Some(header) => header,
            None => {
                headers::insert_initial(&client, schema.name, worker_id).await;
                Header::initial()
            }
        };
        tracing::debug!("store {}:{worker_id} is at {header:?}", schema.name);

        Self {
            client,
            schema: schema.name,
            worker_id,
            header,
            batch_store: B::new().await,
        }
    }

    pub fn get_header(&self) -> &Header {
        &self.header
    }

    pub async fn persist(&mut self, data: &StampedData<<B as BatchStore>::B>) {
        tracing::trace!("persisiting data for {:?}", Header::from(data));
        assert_eq!(self.header.height + 1, data.height);
        assert_eq!(self.header.header_id, data.parent_id);

        // Start db tx
        let pgtx = self.client.transaction().await.unwrap();

        self.batch_store.persist(&pgtx, &data).await;

        // Update header
        self.header = Header::from(data);
        headers::update(&pgtx, &self.schema, &self.worker_id, &self.header).await;

        // Commit db tx
        pgtx.commit().await.unwrap();
    }

    pub async fn roll_back(&mut self, height: Height) {
        tracing::trace!("rolling back height {height}");
        assert_eq!(self.header.height, height);

        let parent_header = core_headers::get(&self.client, &self.header.parent_id)
            .await
            .unwrap();

        // Start db tx
        let pgtx = self.client.transaction().await.unwrap();

        self.batch_store.roll_back(&pgtx, &self.header).await;

        // Update header
        self.header = parent_header;
        headers::update(&pgtx, &self.schema, &self.worker_id, &self.header).await;

        pgtx.commit().await.unwrap();
    }
}

#[async_trait]
pub trait SourcableStore {
    type S;

    /// Get data for given `head`.
    ///
    /// Used by lagging cursors to retrieve data.
    async fn get_at(&self, client: &Client, height: Height) -> Self::S;
}

impl<B: BatchStore + SourcableStore> PgStore<B> {
    /// Returns true if given `header` is part of main chain
    pub async fn is_main_chain(&self, header: &Header) -> bool {
        core_headers::is_main_chain(&self.client, &header).await
    }

    pub async fn get_at(&self, height: Height) -> StampedData<<B as SourcableStore>::S> {
        StampedData {
            height: self.header.height,
            timestamp: self.header.timestamp,
            header_id: self.header.header_id.clone(),
            parent_id: self.header.parent_id.clone(),
            data: self.batch_store.get_at(&self.client, height).await,
        }
    }
}

mod headers {
    use super::Header;
    use tokio_postgres::Client;
    use tokio_postgres::Transaction;

    pub async fn get(client: &Client, schema: &str, worker_id: &str) -> Option<Header> {
        tracing::trace!("get {schema} {worker_id}");
        let qry = format!(
            "
            select height
                , timestamp
                , header_id
                , parent_id
            from {schema}._header
            where worker_id = $1;",
        );
        client
            .query_opt(&qry, &[&worker_id])
            .await
            .unwrap()
            .map(|row| Header {
                height: row.get(0),
                timestamp: row.get(1),
                header_id: row.get(2),
                parent_id: row.get(3),
            })
    }

    /// Insert initial header.
    pub async fn insert_initial(client: &Client, schema: &str, worker_id: &str) {
        tracing::trace!("insert initial {schema} {worker_id}");
        let h = Header::initial();
        let sql = format!(
            "
            insert into {schema}._header (worker_id, height, timestamp, header_id, parent_id)
            values ($1, $2, $3, $4, $5);"
        );
        client
            .execute(
                &sql,
                &[
                    &worker_id,
                    &h.height,
                    &h.timestamp,
                    &h.header_id,
                    &h.parent_id,
                ],
            )
            .await
            .unwrap();
    }

    /// Update `schema` header for given `worker_id`.
    pub async fn update(pgtx: &Transaction<'_>, schema: &str, worker_id: &str, header: &Header) {
        tracing::trace!("update {schema} {worker_id} {header:?}");
        let sql = format!(
            "
            update {schema}._header
            set height = $1
                , timestamp = $2
                , header_id = $3
                , parent_id = $4
            where worker_id = $5;",
        );
        let n_modified = pgtx
            .execute(
                &sql,
                &[
                    &header.height,
                    &header.timestamp,
                    &header.header_id,
                    &header.parent_id,
                    &worker_id,
                ],
            )
            .await
            .unwrap();
        assert_eq!(n_modified, 1);
    }
}

// TODO: move to core::store::headers
mod core_headers {
    use super::Header;
    use tokio_postgres::Client;

    pub async fn get(client: &Client, header_id: &str) -> Option<Header> {
        tracing::trace!("get {header_id}");
        let qry = "
            select height
                , timestamp
                , header_id
                , parent_id
            from core.headers
            where header_id = $1
            order by height desc
            limit 1;";
        client
            .query_opt(qry, &[&header_id])
            .await
            .unwrap()
            .map(|row| Header {
                height: row.get(0),
                timestamp: row.get(1),
                header_id: row.get(2),
                parent_id: row.get(3),
            })
    }

    pub async fn is_main_chain(client: &Client, header: &Header) -> bool {
        tracing::trace!("is_main_chain {header:?}");
        let qry = "
            select main_chain
            from core.headers
            where header_id = $1;";
        match client.query_opt(qry, &[&header.header_id]).await.unwrap() {
            Some(row) => row.get(0),
            None => false,
        }
    }
}

pub struct Schema {
    pub name: &'static str,
    pub sql: &'static str,
}

struct Revision {
    pub major: i32,
    pub minor: i32,
}

impl Schema {
    pub fn new(name: &'static str, sql: &'static str) -> Self {
        Self { name, sql }
    }

    pub async fn init(&self, client: &mut Client) {
        tracing::trace!("initializing schema {}", &self.name);
        if !self.schema_exists(client).await {
            self.load_schema(client).await;
        }
        let rev = self.schema_revision(client).await;
        if rev.major > 1 || rev.minor > 0 {
            todo!("apply miggrations")
        }
    }

    async fn schema_revision(&self, client: &Client) -> Revision {
        tracing::trace!("reading current revision");
        let qry = format!("select rev_major, rev_minor from {}._rev;", self.name);
        match client.query_one(&qry, &[]).await {
            Ok(row) => Revision {
                major: row.get(0),
                minor: row.get(1),
            },
            Err(err) => panic!("{:?}", err),
        }
    }

    async fn schema_exists(&self, client: &Client) -> bool {
        tracing::trace!("checking for existing {} schema", &self.name);
        let qry = "
        select exists(
            select schema_name
            from information_schema.schemata
            where schema_name = $1
        );";
        client.query_one(qry, &[&self.name]).await.unwrap().get(0)
    }

    async fn load_schema(&self, client: &mut Client) {
        tracing::debug!("loading schema {}", &self.name);
        let tx = client.transaction().await.unwrap();
        tx.batch_execute(self.sql).await.unwrap();
        tx.commit().await.unwrap();
    }
}
