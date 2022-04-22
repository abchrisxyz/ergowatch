use crate::parsing::BlockData;
use postgres::Transaction;

/// Find new deposit addresses
pub(super) fn include(tx: &mut Transaction, block: &BlockData) {
    tx.execute(FIND_NEW_ADDRESSES_AT_H, &[&block.height])
        .unwrap();
}

/// Remove deposit addresses
pub(super) fn rollback(tx: &mut Transaction, block: &BlockData) {
    tx.execute(DELETE_NEW_ADDRESSES_AT_H, &[&block.height])
        .unwrap();
}

/// Find new deposit addresses.
///
/// New deposit addresses are addresses sending
/// for the first time ever to one of the main CEX
/// addresses.
const FIND_NEW_ADDRESSES_AT_H: &str = "
    with to_main_txs as ( 
        select cas.cex_id
            , dif.tx_id
            , dif.value
            , cas.address as main_address
        from cex.addresses cas
        join bal.erg_diffs dif on dif.address = cas.address
        where cas.type = 'main'
            and dif.height = $1
            and dif.value > 0
    )
    insert into cex.new_deposit_addresses (address, cex_id, spot_height)
    select dif.address
        , txs.cex_id 
        , $1
    from bal.erg_diffs dif
    join to_main_txs txs on txs.tx_id = dif.tx_id
    -- be aware of known addresses
    left join cex.addresses cas
        on cas.address = dif.address
        and cas.cex_id = txs.cex_id
    -- be aware of recent new addresses
    left join cex.new_deposit_addresses nas
        on nas.address = dif.address
        and nas.cex_id = txs.cex_id
    where dif.value < 0
        and dif.height = $1
        -- exclude txs from known cex addresses
        and cas.address is null
        and nas.address is null
    group by 1, 2;
    ";

pub const DELETE_NEW_ADDRESSES_AT_H: &str = "
    delete from cex.new_deposit_addresses
    where spot_height = $1;";
