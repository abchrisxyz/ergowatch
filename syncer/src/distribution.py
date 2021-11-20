# ------------------------------------------------------------------------------
# Sync dis schema
# ------------------------------------------------------------------------------
from datetime import datetime
from textwrap import dedent
from typing import NamedTuple, List
import logging
import asyncio

import asyncpg as pg

from utils import prep_logger
import addresses
from ergo import circ_supply

logger = logging.getLogger("dis")
prep_logger(logger, level=logging.INFO)


class TopAddressesSupply(NamedTuple):
    top10: int
    top100: int
    top1k: int
    top10k: int
    cexs: int
    dapps: int


class MinBalAddressCounts(NamedTuple):
    total: int
    m_0_001: int
    m_0_01: int
    m_0_1: int
    m_1: int
    m_10: int
    m_100: int
    m_1k: int
    m_10k: int
    m_100k: int
    m_1m: int


async def prepare_unspent_boxes_snapshot(conn: pg.Connection, height: int):
    """
    Prepare unspent boxes snapshot table for given height.
    """
    logger.info(f"Preparing unspent boxes snapshot for block {height}")

    qry = dedent(
        f"""
        create table dis.unspent_boxes_snapshot as
            with inputs as (
                select nis.box_id
                from node_inputs nis
                join node_headers nhs on nhs.id = nis.header_id
                where nhs.main_chain and nis.main_chain
                    and nhs.height <= {height}
            )
            select nos.box_id
            from node_outputs nos
            join node_headers nhs on nhs.id = nos.header_id
            left join inputs nis on nis.box_id = nos.box_id
            where nhs.main_chain and nos.main_chain
                and nis.box_id is null
                -- exclude coinbase
                and nos.address <> '{addresses.coinbase}'
                and nhs.height <= {height};
        """
    )
    await conn.execute(qry)
    await conn.execute(
        "alter table dis.unspent_boxes_snapshot add primary key (box_id);"
    )


async def prepare_address_balances_snapshot(conn: pg.Connection):
    """
    Prepare address balances snapshot.

    Relies on uspent boxes snapshot.
    """
    logger.info(f"Preparing address balances snapshot")

    qry = dedent(
        """
        create table dis.address_balances_snapshot as
            select nos.address,
                sum(nos.value) as value
            from dis.unspent_boxes_snapshot ubs
            join node_outputs nos on nos.box_id = ubs.box_id
            group by 1;
        """
    )

    await conn.execute(qry)
    await conn.execute(
        "alter table dis.address_balances_snapshot add primary key (address);"
    )
    await conn.execute("create index on dis.address_balances_snapshot (value);")


async def update_unspent_boxes_count(conn: pg.Connection, timestamp: int):
    """
    Adds row to unspent boxes tables

    Assumes snapshots are up to date.
    """
    logger.info("Updating unspent boxes count")
    qry = dedent(
        """
        insert into dis.unspent_boxes(timestamp, boxes)
        select $1, count(*)
        from dis.unspent_boxes_snapshot;
        """
    )
    await conn.execute(qry, timestamp)


async def qry_top_addresses_supply(conn: pg.Connection) -> TopAddressesSupply:
    """
    Return current supply held in top x addreses, CEX's and dapps.

    Assumes snapshots are up to date.
    """
    qry = dedent(
        f"""
        with address_balances as (
            select nos.address
                , sum(nos.value) / 10^9 as erg
            from dis.unspent_boxes_snapshot ubs
            join node_outputs nos on nos.box_id = ubs.box_id
            -- exclude treasury
            where nos.address <> '{addresses.treasury}'
            group by 1
        ), ranked_addresses as (
                select row_number() over (order by erg desc) as value_rank
                    , sum(erg) over (order by erg desc rows between unbounded preceding and current row) as erg
                from address_balances bal
                left join dis.cex_addresses cex on cex.address = bal.address
                left join dis.dapp_addresses dap on dap.address = bal.address
                where cex.address is null
                    and dap.address is null
                order by erg desc
        ), cexs as (
            select sum(erg) as erg
            from address_balances bal
            join dis.cex_addresses cex on cex.address = bal.address
        ), dapps as (
            select sum(erg) as erg
            from address_balances bal
            join dis.dapp_addresses dap on dap.address = bal.address
        )
        select
            (select erg from ranked_addresses where value_rank = 10)::int as t10
            ,(select erg from ranked_addresses where value_rank = 100)::int as t100
            ,(select erg from ranked_addresses where value_rank = 1000)::int as t1k
            ,(select erg from ranked_addresses where value_rank = 10000)::int as t10k
            , cex.erg::int as cexs
            , dap.erg::int as dapps
        from cexs cex, dapps dap;
        """
    )

    r = await conn.fetchrow(qry)
    return TopAddressesSupply(
        r["t10"], r["t100"], r["t1k"], r["t10k"], r["cexs"], r["dapps"]
    )


async def insert_top_addresses_supply(
    conn: pg.Connection, timestamp: int, tas: TopAddressesSupply
):
    """
    Add row to top addresses supply table
    """
    qry = dedent(
        """
        insert into dis.top_addresses_supply(timestamp, top10, top100, top1k, top10k, cexs, dapps)
        values ($1, $2, $3, $4, $5, $6, $7);
        """
    )
    await conn.execute(qry, timestamp, *tas)


async def update_top_addresses_supply(conn: pg.Connection, timestamp: int):
    logger.info(f"Updating top addresses supply")
    tas = await qry_top_addresses_supply(conn)
    await insert_top_addresses_supply(conn, timestamp, tas)


async def qry_address_counts_by_minimal_balance(
    conn: pg.Connection,
) -> MinBalAddressCounts:
    """
    Returns current address counts by minimal balance.

    Assumes snapshots are up to date.
    """
    qry = dedent(
        f"""
        select count(*) as total
            , count(*) filter (where value >= 0.001 * 10^9)
            , count(*) filter (where value >= 0.01 * 10^9)
            , count(*) filter (where value >= 0.1 * 10^9)
            , count(*) filter (where value >= 1 * 10^9)
            , count(*) filter (where value >= 10 * 10^9)
            , count(*) filter (where value >= 100 * 10^9)
            , count(*) filter (where value >= 1000 * 10^9)
            , count(*) filter (where value >= 10000 * 10^9)
            , count(*) filter (where value >= 100000 * 10^9)
            , count(*) filter (where value >= 1000000 * 10^9)
        from dis.address_balances_snapshot bal
        left join dis.cex_addresses cex on cex.address = bal.address
        left join dis.dapp_addresses dap on dap.address = bal.address
        where cex.address is null
            and dap.address is null
            -- exclude treasury
            and bal.address <> '{addresses.treasury}';
        """
    )

    r = await conn.fetchrow(qry)
    return MinBalAddressCounts(*r)


async def insert_address_counts_by_minimal_balance(
    conn: pg.Connection, timestamp: int, mbac: MinBalAddressCounts
):
    """
    Add row to address counts table
    """
    qry = dedent(
        """
        insert into dis.address_counts_by_minimal_balance(
            timestamp,
            total,
            m_0_001,
            m_0_01,
            m_0_1,
            m_1,
            m_10,
            m_100,
            m_1k,
            m_10k,
            m_100k,
            m_1m
        )
        values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);
        """
    )
    await conn.execute(qry, timestamp, *mbac)


async def update_address_counts_by_minimal_balance(conn: pg.Connection, timestamp: int):
    logger.info(f"Updating address counts")
    mbac = await qry_address_counts_by_minimal_balance(conn)
    await insert_address_counts_by_minimal_balance(conn, timestamp, mbac)


async def qry_unprocessed_first_of_day_block_heights(conn: pg.Connection) -> List[int]:
    """
    Returns list of heights corresponding to unprocessed first-of-day blocks
    """
    qry = dedent(
        """
        with last_processed_day as (
            select 0 as timestamp -- ensure at least one row when starting from scratch
            union
            select timestamp
            from dis.address_counts_by_minimal_balance
            order by 1 desc
            limit 1
        ), first_of_day_blocks as (
            select extract(year from to_timestamp(nhs.timestamp / 1000)) as y
                , extract(month from to_timestamp(nhs.timestamp / 1000)) as m
                , extract(day from to_timestamp(nhs.timestamp / 1000)) as d
                , min(nhs.height) as height
                , min(nhs.timestamp) as timestamp
            from node_headers nhs, last_processed_day lpd
            where main_chain
                and nhs.timestamp >= lpd.timestamp
            group by 1, 2, 3
        )
        select array_agg(fdb.height)
        from first_of_day_blocks fdb
        -- Keep new blocks only
        left join dis.address_counts_by_minimal_balance acs
            on acs.timestamp = fdb.timestamp
        where acs.timestamp is null
        order by 1;
        """
    )

    r = await conn.fetchrow(qry)
    return r[0] if r[0] is not None else []


async def qry_block_timestamp(conn: pg.Connection, height: int) -> int:
    """
    Get timestamp for given height.
    """
    qry = f"select timestamp from node_headers where main_chain and height = {height};"
    r = await conn.fetchrow(qry)
    return r[0]


async def drop_snapshots(conn: pg.Connection):
    """
    Drop all snapshot tables.
    """
    await conn.execute("drop table dis.unspent_boxes_snapshot;")
    await conn.execute("drop table dis.address_balances_snapshot;")


async def qry_current_block(conn: pg.Connection) -> int:
    """
    Returns height of latest block
    """
    return (
        await conn.fetchrow("select height from node_headers order by 1 desc limit 1;")
    )[0]


async def update_preview(conn: pg.Connection, timestamp: int, height: int):
    logger.info(f"Updating preview")
    await conn.execute("truncate dis.preview;")

    cs = circ_supply(height)

    qry = dedent(
        f"""
        insert into dis.preview(timestamp, total_addresses, top100_supply_fraction, boxes)
            select
                {timestamp},
                (
                    select total
                    from dis.address_counts_by_minimal_balance
                    order by timestamp desc limit 1
                ) as total_addresses,
                (
                    select top100 / {cs}::numeric
                    from dis.top_addresses_supply
                    order by timestamp desc limit 1
                ) as top100,
                (
                    select boxes
                    from dis.unspent_boxes
                    order by timestamp desc limit 1
                ) as utxos
        ;
        """
    )
    await conn.execute(qry)


async def update_address_counts_summary(conn: pg.Connection, height: int):
    logger.info(f"Updating address counts summary")
    await conn.execute("truncate dis.address_counts_summary;")

    cs = circ_supply(height)

    template = dedent(
        """
        insert into dis.address_counts_summary(label, latest, diff_1d, diff_1w, diff_4w, diff_6m, diff_1y)
            select '{0}' as label
                , {0} as latest
                , {0} - lead({0}, 1) over (order by timestamp desc) as diff_1d
                , {0} - lead({0}, 7) over (order by timestamp desc) as diff_7d
                , {0} - lead({0}, 28) over (order by timestamp desc) as diff_4w
                , {0} - lead({0}, 183) over (order by timestamp desc) as diff_6m
                , {0} - lead({0}, 365) over (order by timestamp desc) as diff_1y
            from dis.address_counts_by_minimal_balance
            order by timestamp desc
            limit 1;
        """
    )

    columns = [
        "total",
        "m_0_001",
        "m_0_01",
        "m_0_1",
        "m_1",
        "m_10",
        "m_100",
        "m_1k",
        "m_10k",
        "m_100k",
        "m_1m",
    ]
    for col in columns:
        qry = template.format(col)
        await conn.execute(qry)


async def sync(conn: pg.Connection):
    """
    Main sync function.
    """
    logger.info("Syncing started")

    heights = await qry_unprocessed_first_of_day_block_heights(conn)

    # Filter out any fresh blocks.
    # Just to make sure we process blocks once they have enough confirmations.
    min_confirmations = 10
    current_height = await qry_current_block(conn)
    heights = [h for h in heights if current_height - h >= min_confirmations]

    logger.info(f"Number of blocks to process: {len(heights)}")

    for h in heights:
        async with conn.transaction():
            logger.info(f"Processing block {h}")

            timestamp = await qry_block_timestamp(conn, h)
            logger.info(
                f"Block {h} has timestamp {datetime.utcfromtimestamp(timestamp / 1000)}"
            )

            await prepare_unspent_boxes_snapshot(conn, h)
            await prepare_address_balances_snapshot(conn)

            await update_unspent_boxes_count(conn, timestamp)
            await update_top_addresses_supply(conn, timestamp)
            await update_address_counts_by_minimal_balance(conn, timestamp)
            await update_preview(conn, timestamp, h)
            await update_address_counts_summary(conn, h)

            await drop_snapshots(conn)

    logger.info("Syncing completed")


async def main():
    """
    Convenience wrapper to call sync() on it's own.

    Usefull when boostrapping the db.
    """
    conn = await pg.connect(DBSTR)
    await sync(conn)
    await conn.close()


if __name__ == "__main__":
    from local import DBSTR

    asyncio.get_event_loop().run_until_complete(main())
