/// Migration 6
///
/// Reboot cex schema with modified Gate addresses and ignore list.
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    let statements = vec![
        "drop schema cex cascade;",
        "create schema cex;",
        "create table cex.cexs (
            id integer,
            name text
        );",
        "insert into cex.cexs (id, name) values
            (1, 'Coinex'),
            (2, 'Gate.io'),
            (3, 'KuCoin'),
            (4, 'ProBit')
        ;",
        "create type cex.t_address_type as enum ('main', 'deposit');",
        "create table cex.addresses (
            address text,
            cex_id integer,
            type cex.t_address_type,
            spot_height int
        );",
        "create table cex.addresses_ignored (
            address text
        );",
        "create table cex.addresses_conflicts (
            -- Same columns as cex.address
            address text,
            first_cex_id integer,
            type cex.t_address_type,
            spot_height integer,
            -- Then some info on when the conflict occurred
            conflict_spot_height integer
        );",
        "create type cex.t_block_status as enum (
            'pending',
            'pending_rollback',
            'processed',
            'processed_rollback'
        );",
        "create table cex.block_processing_log (
            header_id text,
            height integer,
            invalidation_height integer,
            status cex.t_block_status
        );",
        "create table cex.supply (
            height int,
            cex_id integer,
            main bigint,
            deposit bigint
        );",
        "insert into cex.addresses (cex_id, type, address) values
            (1, 'main', '9fowPvQ2GXdmhD2bN54EL9dRnio3kBQGyrD3fkbHwuTXD6z1wBU'),
            (1, 'main', '9fPiW45mZwoTxSwTLLXaZcdekqi72emebENmScyTGsjryzrntUe'),
            (2, 'main', '9iKFBBrryPhBYVGDKHuZQW7SuLfuTdUJtTPzecbQ5pQQzD4VykC'),
            (2, 'main', '9gQYrh6yubA4z55u4TtsacKnaEteBEdnY4W2r5BLcFZXcQoQDcq'),
            (2, 'main', '9enQZco9hPuqaHvR7EpPRWvYbkDYoWu3NK7pQk8VFwgVnv5taQE'),
            (3, 'main', '9hU5VUSUAmhEsTehBKDGFaFQSJx574UPoCquKBq59Ushv5XYgAu'),
            (3, 'main', '9i8Mci4ufn8iBQhzohh4V3XM3PjiJbxuDG1hctouwV4fjW5vBi3'),
            (3, 'main', '9guZaxPoe4jecHi6ZxtMotKUL4AzpomFf3xqXsFSuTyZoLbmUBr'),
            (3, 'main', '9iNt6wfxSc3DSaBVp22E7g993dwKUCvbGdHoEjxF8SRqj35oXvT'),
            (4, 'main', '9eg2Rz3tGogzLaVZhG1ycPj1dJtN4Jn8ySa2mnVLJyVJryb13QB');",
        "insert into cex.addresses_ignored (address) values
            ('9hxFS2RkmL5Fv5DRZGwZCbsbjTU1R75Luc2t5hkUcR1x3jWzre4'),
            ('9gNYeyfRFUipiWZ3JR1ayDMoeh28E6J7aDQosb7yrzsuGSDqzCC'),
            ('9i2oKu3bbHDksfiZjbhAgSAWW7iZecUS78SDaB46Fpt2DpUNe6M'),
            ('9iHCMtd2gAPoYGhWadjruygKwNKRoeQGq1xjS2Fkm5bT197YFdR');",
    ];

    for statement in statements {
        tx.execute(statement, &[])?;
    }

    Ok(())
}
