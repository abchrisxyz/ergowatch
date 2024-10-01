use async_trait::async_trait;
use std::fmt;
use tokio_postgres::Client;
use tokio_postgres::NoTls;
use tokio_postgres::Transaction;

use super::utils::BlockRange;
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

    pub fn get_mut_client(&mut self) -> &mut Client {
        &mut self.client
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

        // Check revision
        let rev = revisions::get(&client, &store).await;
        if &rev != store.revision {
            panic!("Store revision is lagging. Ensure all migrations have been applied.")
        }

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

    /// Get data for given height range.
    ///
    /// Used by lagging cursors to retrieve data.
    async fn get_slice(&self, client: &Client, block_range: &BlockRange) -> Vec<Self::S>;
}

impl<B: BatchStore + SourcableStore> PgStore<B> {
    /// Returns true if given `header` is part of main chain
    pub async fn is_main_chain(&self, header: &Header) -> bool {
        core_headers::is_main_chain(&self.client, &header).await
    }

    pub async fn get_slice(
        &self,
        block_range: &BlockRange,
    ) -> Vec<StampedData<<B as SourcableStore>::S>> {
        let headers = core_headers::get_slice(&self.client, block_range).await;
        let datas = self.batch_store.get_slice(&self.client, block_range).await;
        headers
            .into_iter()
            .zip(datas.into_iter())
            .map(|(h, d)| StampedData::new(h, d))
            .collect()
    }
}

pub trait PatchableStore {
    type P; // Patch type
    fn stage_rollback_patch(&mut self, patch: Self::P);
}

impl<S: BatchStore + PatchableStore> PgStore<S> {
    pub fn stage_rollback_patch(&mut self, patch: S::P) {
        self.batch_store.stage_rollback_patch(patch);
    }
}

pub struct StoreDef {
    pub schema_name: &'static str,
    pub worker_id: &'static str,
    pub sql: &'static str,
    pub revision: &'static Revision,
}

#[derive(Debug, PartialEq, Eq)]
pub struct Revision {
    pub major: i32,
    pub minor: i32,
}

impl Revision {
    pub fn new(major: i32, minor: i32) -> Self {
        Self { major, minor }
    }
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
            headers::insert_initial(&mut pgtx, &self.schema_name, &self.worker_id).await;
            pgtx.commit().await.unwrap();
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

    pub(super) async fn update(
        pgtx: &Transaction<'_>,
        schema_name: &str,
        worker_id: &str,
        rev: &Revision,
    ) {
        tracing::trace!("update {schema_name} to {rev:?}");
        let sql = "
            update ew.revisions
            set major = $1, minor = $2
            where schema_name = $3 and worker_id = $4;";
        assert_eq!(
            pgtx.execute(sql, &[&rev.major, &rev.minor, &schema_name, &worker_id])
                .await
                .unwrap(),
            1
        );
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
    pub(super) async fn insert_initial(pgtx: &Transaction<'_>, schema: &str, worker_id: &str) {
        tracing::trace!("insert initial for {schema}/{worker_id}");
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
                &schema,
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

    pub(super) async fn delete(pgtx: &Transaction<'_>, schema: &str, worker_id: &str) {
        let sql = "delete from ew.headers where schema_name = $1 and worker_id = $2;";
        let n_deleted = pgtx.execute(sql, &[&schema, &worker_id]).await.unwrap();
        assert_eq!(n_deleted, 1);
    }
}

// TODO: move to core::store::headers
mod core_headers {
    use super::BlockRange;
    use super::Header;
    use super::Height;
    use tokio_postgres::Client;
    use tokio_postgres::Transaction;

    /// Get header with given `header_id`
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

    /// Get main chain header for given `height`
    pub async fn get_main_at(pgtx: &Transaction<'_>, height: Height) -> Option<Header> {
        tracing::trace!("get_main_at {height}");
        let qry = "
            select height
                , timestamp
                , header_id
                , parent_id
            from core.headers
            where height = $1
                and main_chain;";
        pgtx.query_opt(qry, &[&height])
            .await
            .unwrap()
            .map(|row| Header {
                height: row.get(0),
                timestamp: row.get(1),
                header_id: row.get(2),
                parent_id: row.get(3),
            })
    }

    /// Get main chain headers for given `block_range`
    pub async fn get_slice(client: &Client, block_range: &BlockRange) -> Vec<Header> {
        tracing::trace!("get_at {block_range:?}");
        let qry = "
            select height
                , timestamp
                , header_id
                , parent_id
            from core.headers
            where height >= $1
                and height <= $2
                and main_chain;
        ";
        client
            .query(qry, &[&block_range.first_height, &block_range.last_height])
            .await
            .unwrap()
            .iter()
            .map(|row| Header {
                height: row.get(0),
                timestamp: row.get(1),
                header_id: row.get(2),
                parent_id: row.get(3),
            })
            .collect()
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

#[async_trait]
pub trait Migration: std::fmt::Debug {
    fn description(&self) -> &'static str;

    fn revision(&self) -> Revision;

    async fn run(&self, pgtx: &Transaction<'_>) -> MigrationEffect;
}

/// Describes migration effect on store height and modifies `ew.headers` table accordingly.
pub enum MigrationEffect {
    /// No content changes.
    None,
    /// Store got rolled back to height.
    Trimmed(Height),
    /// Store got emptied and needs to sync from scratch.
    /// Will cause store header to be removed from `ew.headers` table.
    Reset,
}

/// Applies migrations to a PgStore.
pub struct PgMigrator {
    client: Client,
    schema: &'static str,
    worker_id: &'static str,
    revision: Revision,
}

impl PgMigrator {
    pub async fn new(pgconf: &PostgresConfig, store: &StoreDef) -> Self {
        tracing::debug!("initializing migrator {store}");

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
        let revision = revisions::get(&client, store).await;
        tracing::debug!("store {store} has revision {revision:?}");

        Self {
            client,
            schema: store.schema_name,
            worker_id: store.worker_id,
            revision,
        }
    }

    /// Execute migration in a single transaction
    #[tracing::instrument(name = "migration", skip(self))]
    pub async fn apply(&mut self, mig: &impl Migration) {
        tracing::trace!("evaluating migration {:?}", mig.revision());
        // Major migrations not supported yet
        assert_eq!(mig.revision().major, self.revision.major);

        // Skip if migration already applied
        if mig.revision().minor <= self.revision.minor {
            tracing::trace!("skipping migration {:?}", mig.revision());
            return;
        }

        // Check migration to be applied is next in line.
        assert_eq!(mig.revision().minor, self.revision.minor + 1);
        tracing::info!(
            "applying {} migration {:?} - {}",
            self.worker_id,
            mig.revision(),
            mig.description(),
        );

        // Starting db transaction
        let pgtx = self.client.transaction().await.unwrap();

        // Apply migration
        let effect = mig.run(&pgtx).await;

        // Reflect migration in store's header
        match effect {
            MigrationEffect::None => {
                // Nothing to do
            }
            MigrationEffect::Trimmed(height) => {
                let header = core_headers::get_main_at(&pgtx, height).await.unwrap();
                headers::update(&pgtx, self.schema, self.worker_id, &header).await;
            }
            MigrationEffect::Reset => {
                // Reset worker by resetting its header
                headers::delete(&pgtx, self.schema, self.worker_id).await;
                headers::insert_initial(&pgtx, self.schema, self.worker_id).await;
            }
        };

        // Update store's revision
        self.revision = mig.revision();
        revisions::update(&pgtx, self.schema, self.worker_id, &self.revision).await;

        // Commit db transaction
        pgtx.commit().await.unwrap();
    }
}
