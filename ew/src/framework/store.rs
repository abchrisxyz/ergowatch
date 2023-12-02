use async_trait::async_trait;
use std::fmt;
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

    pub async fn new(pgconf: &PostgresConfig, store: &StoreDef) -> Self {
        tracing::debug!("initializing store {store}");

        // init client
        let (mut client, connection) = tokio_postgres::connect(&pgconf.connection_uri, NoTls)
            .await
            .unwrap();

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        // Prepare store schema if needed
        store.init(&mut client).await;

        // Retrieve header
        let header = headers::get(&client, store.schema_name, store.worker_id).await;
        tracing::debug!("store {store} is at {header:?}",);

        Self {
            client,
            schema: store.schema_name,
            worker_id: store.worker_id,
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

pub struct StoreDef {
    pub schema_name: &'static str,
    pub worker_id: &'static str,
    pub sql: &'static str,
    pub revision: &'static Revision,
}

pub struct Revision {
    pub major: i32,
    pub minor: i32,
}

impl StoreDef {
    /// Initialize (worker's part of) schema if not declared yet.
    ///
    /// Executes schema's sql and add initial header
    pub async fn init(&self, client: &mut Client) {
        tracing::trace!("initializing schema {self}");

        // First off, check if ew schema is present.
        if !schema_exists(client, "ew").await {
            tracing::debug!("loading ew schema");
            let tx = client.transaction().await.unwrap();
            tx.batch_execute(include_str!("ew.sql")).await.unwrap();
            tx.commit().await.unwrap();
        }

        if !self.is_initialized(client).await {
            tracing::debug!("loading schema for {self}");
            let mut pgtx = client.transaction().await.unwrap();
            pgtx.batch_execute(self.sql).await.unwrap();
            revisions::insert(&mut pgtx, &self).await;
            headers::insert_initial(&mut pgtx, &self).await;
            pgtx.commit().await.unwrap();
        }
        // Check revision
        let rev = revisions::get(&client, &self).await;
        if rev.major > 1 || rev.minor > 0 {
            todo!("apply miggrations")
        }
    }

    /// Checks if the store's relations have been initialized already.
    ///
    /// Checks for presence of schema/worker_id pair in the ew.revisions table.
    async fn is_initialized(&self, client: &Client) -> bool {
        tracing::trace!("checking store initialization for {self}",);
        let qry = "
            select exists(
                select *
                from ew.revisions
                where schema_name = $1 and worker_id = $2
        );";
        client
            .query_one(qry, &[&self.schema_name, &self.worker_id])
            .await
            .unwrap()
            .get(0)
    }
}

impl fmt::Display for StoreDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.schema_name, self.worker_id)
    }
}

/// Access ew.ervisions table
mod revisions {
    use tokio_postgres::Client;
    use tokio_postgres::Transaction;

    use super::Revision;
    use super::StoreDef;

    pub(super) async fn get(client: &Client, store: &StoreDef) -> Revision {
        let qry = "
            select major, minor
            from ew.revisions
            where schema_name = $1 and worker_id = $2;
        ";
        // Revision is set during schema declaration, so guaranteed to be present.
        let row = client
            .query_one(qry, &[&store.schema_name, &store.worker_id])
            .await
            .unwrap();
        Revision {
            major: row.get(0),
            minor: row.get(1),
        }
    }

    pub(super) async fn insert(pgtx: &mut Transaction<'_>, schema: &StoreDef) {
        tracing::trace!("insert for {schema}");
        let sql = "
            insert into ew.revisions (schema_name, worker_id, major, minor)
            values ($1, $2, $3, $4);";
        pgtx.execute(
            sql,
            &[
                &schema.schema_name,
                &schema.worker_id,
                &schema.revision.major,
                &schema.revision.minor,
            ],
        )
        .await
        .unwrap();
    }
}

/// Access ew.headers table
mod headers {
    use super::Header;
    use super::StoreDef;
    use tokio_postgres::Client;
    use tokio_postgres::Transaction;

    /// Get header for given `schema` and `worker_id`.
    pub(super) async fn get(client: &Client, schema: &str, worker_id: &str) -> Header {
        tracing::trace!("get {schema} {worker_id}");
        let qry = "
            select height
                , timestamp
                , header_id
                , parent_id
            from ew.headers
            where schema_name = $1 and worker_id = $2;";
        let row = client.query_one(qry, &[&schema, &worker_id]).await.unwrap();
        Header {
            height: row.get(0),
            timestamp: row.get(1),
            header_id: row.get(2),
            parent_id: row.get(3),
        }
    }

    /// Insert initial header.
    pub(super) async fn insert_initial(pgtx: &Transaction<'_>, store: &StoreDef) {
        tracing::trace!("insert initial for {store}");
        let h = Header::initial();
        let sql = "
            insert into ew.headers (schema_name, worker_id, height, timestamp, header_id, parent_id)
            values ($1, $2, $3, $4, $5, $6)
            -- Workers that don't start at genesis will have set their header already
            on conflict do nothing
            ;";
        pgtx.execute(
            sql,
            &[
                &store.schema_name,
                &store.worker_id,
                &h.height,
                &h.timestamp,
                &h.header_id,
                &h.parent_id,
            ],
        )
        .await
        .unwrap();
    }

    /// Update header for given `schema` and `worker_id`.
    pub(super) async fn update(
        pgtx: &Transaction<'_>,
        schema: &str,
        worker_id: &str,
        header: &Header,
    ) {
        tracing::trace!("update {schema} {worker_id} {header:?}");
        let sql = "
            update ew.headers
            set height = $1
                , timestamp = $2
                , header_id = $3
                , parent_id = $4
            where schema_name = $5 and worker_id = $6;";
        let n_modified = pgtx
            .execute(
                sql,
                &[
                    &header.height,
                    &header.timestamp,
                    &header.header_id,
                    &header.parent_id,
                    &schema,
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

/// Returns True if a schema with given `name` exists.
async fn schema_exists(client: &Client, name: &str) -> bool {
    tracing::trace!("checking for existing {} schema", &name);
    let qry = "
    select exists(
        select schema_name
        from information_schema.schemata
        where schema_name = $1
    );";
    client.query_one(qry, &[&name]).await.unwrap().get(0)
}
