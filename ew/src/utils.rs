use tokio_postgres::Client;

pub struct Schema {
    name: String,
    sql: &'static str,
}

struct Revision {
    pub major: i32,
    pub minor: i32,
}

impl Schema {
    pub fn new(name: &str, sql: &'static str) -> Self {
        Self {
            name: name.to_owned(),
            sql: sql,
        }
    }

    pub async fn init(&self, client: &mut Client) {
        if !self.schema_exists(client).await {
            self.load_schema(client).await;
        }
        let rev = self.schema_revision(client).await;
        if rev.major > 1 || rev.minor > 0 {
            todo!("apply miggrations")
        }
    }

    async fn schema_revision(&self, client: &Client) -> Revision {
        tracing::debug!("reading current revision");
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
        tracing::debug!("checking for existing schema");
        let qry = "
        select exists(
            select schema_name
            from information_schema.schemata
            where schema_name = $1
        );";
        client.query_one(qry, &[&self.name]).await.unwrap().get(0)
    }

    async fn load_schema(&self, client: &mut Client) {
        tracing::debug!("loading schema");
        let tx = client.transaction().await.unwrap();
        tx.batch_execute(self.sql).await.unwrap();
        tx.commit().await.unwrap();
    }
}
