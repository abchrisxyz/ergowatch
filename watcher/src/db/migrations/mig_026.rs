/// Migration 26
///
/// Add TradeOgre
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute(
        "
        insert into cex.cexs (id, name, text_id) values
            (5, 'TradeOgre', 'tradeogre');",
        &[],
    )?;
    tx.execute(
        "
        insert into cex.main_addresses_list (cex_id, address) values
            (5, '9fs99SejQxDjnjwrZ13YMZZ3fwMEVXFewpWWj63nMhZ6zDf2gif');",
        &[],
    )?;
    // Normally would delete records in cex dependent tables up to height of
    // first TradeOgre tx. However, since v0.5 will require a full resync, can
    // skip this time.
    Ok(())
}
