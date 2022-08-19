/// Migration 4
///
/// Add main exchange addresses
use postgres::Transaction;

pub(super) fn apply(tx: &mut Transaction) -> anyhow::Result<()> {
    tx.execute("create schema cex;", &[])?;
    tx.execute("create table cex.cexs ( id integer, name text );", &[])?;
    tx.execute(
        "create type cex.t_address_type as enum ('main', 'deposit');",
        &[],
    )?;
    tx.execute(
        "create table cex.addresses (
            address_id bigint,
            cex_id integer,
            type cex.t_address_type,
            spot_height integer
        );",
        &[],
    )?;
    tx.execute(
        "create table cex.main_addresses_list (
            address text,
            cex_id integer
        );",
        &[],
    )?;
    tx.execute(
        "create table cex.addresses_conflicts (
            address_id bigint,
            first_cex_id integer,
            type cex.t_address_type,
            spot_height integer,
            conflict_spot_height integer
        );",
        &[],
    )?;
    tx.execute(
        "create type cex.t_block_status as enum (
            'pending',
            'pending_rollback',
            'processing',
            'processing_rollback',
            'processed',
            'processed_rollback'
        );",
        &[],
    )?;
    tx.execute(
        "create table cex.block_processing_log (
            header_id text,
            height integer,
            invalidation_height integer,
            status cex.t_block_status not null
        );",
        &[],
    )?;
    tx.execute(
        "insert into cex.cexs (id, name) values
            (1, 'Coinex'),
            (2, 'Gate.io'),
            (3, 'KuCoin'),
            (4, 'ProBit')
        ;",
        &[],
    )?;
    tx.execute(
        "insert into cex.main_addresses_list (cex_id, address) values
            -- Coinex
            (1, '9fowPvQ2GXdmhD2bN54EL9dRnio3kBQGyrD3fkbHwuTXD6z1wBU'),
            (1, '9fPiW45mZwoTxSwTLLXaZcdekqi72emebENmScyTGsjryzrntUe'),
                 
            -- Gate - confirmed
            (2, '9iKFBBrryPhBYVGDKHuZQW7SuLfuTdUJtTPzecbQ5pQQzD4VykC'),
            -- Gate - unconfirmed
            (2, '9enQZco9hPuqaHvR7EpPRWvYbkDYoWu3NK7pQk8VFwgVnv5taQE'),
            (2, '9i7134eY3zUotQyS8nBeZDJ3SWbTPn117nCJYi977FBn9AaxhZY'),
            (2, '9gmb745thQTyoGGWxSr9hNmvipivgVbQGA6EJnBucs3nwi9yqoc'),
            (2, '9fJzuyVaRLM9Q3RZVzkau1GJVP9TDiW8GRL5p25VZ8VNXurDpaw'),
            (2, '9i1ETULiCnGMtppDAvrcYujhxX18km3ge9ZEDMnZPN6LFQbttRF'),
            (2, '9gck4LwHJK3XV2wXdYdN5S9Fe4RcFrkaqs4WU5aeiKuodJyW7qq'),
            (2, '9gv4qw7RtQyt3khtnQNxp7r7yuUazWWyTGfo7duqGj9hMtZxKP1'),
            -- created 10/2019, but listing only in 10/2020 ?
            (2, '9exS2B892HTiDkqhcWnj1nzsbYmVn7ameVb1d2jagUWTqaLxfTX'),

            -- KuCoin
            (3, '9hU5VUSUAmhEsTehBKDGFaFQSJx574UPoCquKBq59Ushv5XYgAu'),
            (3, '9i8Mci4ufn8iBQhzohh4V3XM3PjiJbxuDG1hctouwV4fjW5vBi3'),
            (3, '9guZaxPoe4jecHi6ZxtMotKUL4AzpomFf3xqXsFSuTyZoLbmUBr'),
            (3, '9iNt6wfxSc3DSaBVp22E7g993dwKUCvbGdHoEjxF8SRqj35oXvT'),
            
            -- ProBit
            (4, '9eg2Rz3tGogzLaVZhG1ycPj1dJtN4Jn8ySa2mnVLJyVJryb13QB')
        ;",
        &[],
    )?;
    Ok(())
}
