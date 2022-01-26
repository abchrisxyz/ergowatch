use super::BlockData;
use crate::db::core::header::HeaderRow;
use crate::db::SQLStatement;

// Convert block header to sql statement
pub(super) fn extract_header(block: &BlockData) -> SQLStatement {
    HeaderRow {
        height: block.height,
        id: block.header_id,
        parent_id: block.parent_header_id,
        timestamp: block.timestamp,
    }
    .to_statement()
}

#[cfg(test)]
mod tests {
    use super::extract_header;
    use crate::db;
    use crate::db::SQLArg;
    use crate::units::testing::block_600k;

    #[test]
    fn header_statement() -> () {
        let stmnt = extract_header(&block_600k());
        assert_eq!(stmnt.sql, db::core::header::INSERT_HEADER);
        assert_eq!(stmnt.args[0], SQLArg::Integer(600000));
        assert_eq!(
            stmnt.args[1],
            SQLArg::Text(String::from(
                "5cacca81066cb5ffd64e26096fd6ad4b6b590e7a3c09208bfda79779a7ab90a4"
            ))
        );
        assert_eq!(
            stmnt.args[2],
            SQLArg::Text(String::from(
                "eac9b85b5faca84fda89ed344730488bf11c5689165e04a059bf523776ae39d1"
            ))
        );
        assert_eq!(stmnt.args[3], SQLArg::BigInt(1634511451404));
    }
}
