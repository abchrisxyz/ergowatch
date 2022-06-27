use crate::parsing::BlockData;
use postgres::Transaction;

pub(super) struct Header<'a> {
    height: i32,
    id: &'a str,
    parent_id: &'a str,
    timestamp: i64,
    votes: [i16; 3],
}

pub(super) fn include(tx: &mut Transaction, block: &BlockData) {
    let sql = "
        insert into core.headers (height, id, parent_id, timestamp, vote1, vote2, vote3)
        values ($1, $2, $3, $4, $5, $6, $7);";

    let header = extract_header(block);

    tx.execute(
        sql,
        &[
            &header.height,
            &header.id,
            &header.parent_id,
            &header.timestamp,
            &header.votes[0],
            &header.votes[1],
            &header.votes[2],
        ],
    )
    .unwrap();
}

pub(super) fn rollback(tx: &mut Transaction, block: &BlockData) {
    let sql = "delete from core.headers where id = $1;";
    tx.execute(sql, &[&block.header_id]).unwrap();
}

// Convert block header to sql statement
fn extract_header<'a>(block: &'a BlockData) -> Header<'a> {
    Header {
        height: block.height,
        id: block.header_id,
        parent_id: block.parent_header_id,
        timestamp: block.timestamp,
        // Postgres has no single byte int, so convert to smallint
        votes: [
            block.votes[0] as i16,
            block.votes[1] as i16,
            block.votes[2] as i16,
        ],
    }
}

pub(super) fn set_constraints(tx: &mut Transaction) {
    let statements = vec![
        "alter table core.headers add primary key (height);",
        "alter table core.headers alter column height set not null;",
        "alter table core.headers alter column id set not null;",
        "alter table core.headers alter column parent_id set not null;",
        "alter table core.headers alter column timestamp set not null;",
        "alter table core.headers alter column vote1 set not null;",
        "alter table core.headers alter column vote2 set not null;",
        "alter table core.headers alter column vote3 set not null;",
        "alter table core.headers add constraint headers_unique_id unique(id);",
        "alter table core.headers add constraint headers_unique_parent_id unique(parent_id);",
    ];

    for statement in statements {
        tx.execute(statement, &[]).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::extract_header;
    use crate::parsing::testing::block_600k;

    #[test]
    fn header_data() -> () {
        let block = block_600k();
        let h = extract_header(&block);
        assert_eq!(h.height, 600000);
        assert_eq!(
            h.id,
            "5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4"
        );
        assert_eq!(
            h.parent_id,
            "eac9b85b5faca84fda89ed344730488bf11c5689165e04a059bf523776ae39d1"
        );
        assert_eq!(h.timestamp, 1634511451404);
    }
}
