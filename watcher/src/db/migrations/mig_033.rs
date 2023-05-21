/// Migration 33
///
/// Add main cex address for Kucoin
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    // Reset cex schema
    tx.execute("drop schema cex cascade;", &[])?;

    tx.execute("create schema cex", &[])?;

    tx.execute(
        "
        create table cex.cexs (
            id integer,
            text_id text, -- used for easier api access
            name text
        );",
        &[],
    )?;

    tx.execute(
        "
        insert into cex.cexs (id, name, text_id) values
            (1, 'Coinex', 'coinex'),
            (2, 'Gate.io', 'gate'),
            (3, 'KuCoin', 'kucoin'),
            (4, 'ProBit', 'probit'),
            (5, 'TradeOgre', 'tradeogre'),
            (6, 'Huobi', 'huobi');",
        &[],
    )?;

    tx.execute(
        "
        create table cex.main_addresses (
            address_id bigint,
            cex_id integer
        );",
        &[],
    )?;

    tx.execute(
        "
        create table cex.deposit_addresses (
            address_id bigint,
            cex_id integer,
            spot_height int
        );",
        &[],
    )?;

    tx.execute(
        "
        create table cex.deposit_addresses_excluded (
            address_id bigint,
            address_spot_height integer,
            conflict_spot_height integer
        );",
        &[],
    )?;

    tx.execute(
        "
        create table cex._deposit_addresses_log (
            singleton int primary key default 1,
            last_processed_height int default 0,
            check(singleton = 1)
        );",
        &[],
    )?;
    tx.execute(
        "insert into cex._deposit_addresses_log(singleton) values (1);",
        &[],
    )?;

    tx.execute(
        "
        create table cex.deposit_addresses_ignored (
            address_id bigint
        );",
        &[],
    )?;

    tx.execute(
        "
        create table cex.main_addresses_list (
            address text,
            cex_id integer
        );",
        &[],
    )?;

    tx.execute(
        "
        create table cex.ignored_addresses_list (
            address text
        );",
        &[],
    )?;

    tx.execute(
        "
        create table cex.supply (
            height int,
            cex_id integer,
            main bigint,
            deposit bigint
        );",
        &[],
    )?;

    tx.execute(
        "
        insert into cex.main_addresses_list (cex_id, address) values
            (1, '9fowPvQ2GXdmhD2bN54EL9dRnio3kBQGyrD3fkbHwuTXD6z1wBU'),
            (1, '9fPiW45mZwoTxSwTLLXaZcdekqi72emebENmScyTGsjryzrntUe'),			
            (2, '9iKFBBrryPhBYVGDKHuZQW7SuLfuTdUJtTPzecbQ5pQQzD4VykC'),
            (2, '9gQYrh6yubA4z55u4TtsacKnaEteBEdnY4W2r5BLcFZXcQoQDcq'),
            (2, '9enQZco9hPuqaHvR7EpPRWvYbkDYoWu3NK7pQk8VFwgVnv5taQE'),
            (3, '9hU5VUSUAmhEsTehBKDGFaFQSJx574UPoCquKBq59Ushv5XYgAu'),
            (3, '9i8Mci4ufn8iBQhzohh4V3XM3PjiJbxuDG1hctouwV4fjW5vBi3'),
            (3, '9guZaxPoe4jecHi6ZxtMotKUL4AzpomFf3xqXsFSuTyZoLbmUBr'),
            (3, '9iNt6wfxSc3DSaBVp22E7g993dwKUCvbGdHoEjxF8SRqj35oXvT'),
            (3, '9fs7HkPGY9fhN6WsHd7V7LMcuMqsgseXzNahyToxJKwHCc1zc1c'),
            (4, '9eg2Rz3tGogzLaVZhG1ycPj1dJtN4Jn8ySa2mnVLJyVJryb13QB'),
            (5, '9fs99SejQxDjnjwrZ13YMZZ3fwMEVXFewpWWj63nMhZ6zDf2gif'),
            (6, '9feMGM1qwNG8NnNuk3pz4yeCGm59s2RbjFnS7DxwUxCbzUrNnJw');",
        &[],
    )?;

    tx.execute(
        "
        insert into cex.ignored_addresses_list (address) values
            ('9hxFS2RkmL5Fv5DRZGwZCbsbjTU1R75Luc2t5hkUcR1x3jWzre4'),
            ('9gNYeyfRFUipiWZ3JR1ayDMoeh28E6J7aDQosb7yrzsuGSDqzCC'),
            ('9i2oKu3bbHDksfiZjbhAgSAWW7iZecUS78SDaB46Fpt2DpUNe6M'),
            ('9iHCMtd2gAPoYGhWadjruygKwNKRoeQGq1xjS2Fkm5bT197YFdR');
            ",
        &[],
    )?;

    // Reset cex supply metrics
    tx.execute("drop table mtr.cex_supply;", &[])?;
    tx.execute(
        "
        create table mtr.cex_supply (
            height int,
            total bigint,
            deposit bigint
        );",
        &[],
    )?;

    // Reset supply composition metrics
    tx.execute("drop table mtr.supply_composition;", &[])?;
    tx.execute("drop table mtr.supply_composition_summary;", &[])?;
    tx.execute(
        "
        create table mtr.supply_composition (
            height int,
            -- supply on p2pk addresses, excluding main cex addresses
            p2pks bigint,
            -- supply on main cex addresses
            cex_main bigint,
            -- supply on cex deposit addresses
            cex_deposits bigint,
            -- contracts excluding treasury
            contracts bigint,
            -- all supply on miner addresses, including destined to reemission.
            miners bigint,
            -- unlocked treasury supply (boils down to treasury balance after first 2.5 years)
            treasury bigint
        );",
        &[],
    )?;
    tx.execute(
        "
        create table mtr.supply_composition_summary (
            label text not null primary key,
            current bigint not null,
            diff_1d bigint not null,
            diff_1w bigint not null,
            diff_4w bigint not null,
            diff_6m bigint not null,
            diff_1y bigint not null
        );",
        &[],
    )?;
    tx.execute(
        "update mtr._log set supply_composition_bootstrapped = FALSE; ",
        &[],
    )?;
    tx.execute(
        "update mtr._log set supply_composition_constraints_set = FALSE; ",
        &[],
    )?;

    Ok(())
}
