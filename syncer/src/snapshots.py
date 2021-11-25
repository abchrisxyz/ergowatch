"""
Prepares snapshots of unspent boxes and address balances to be used by
snapshot based metrics.
"""
from datetime import datetime
from textwrap import dedent
from typing import NamedTuple, List
import logging
import asyncio

import asyncpg as pg

from utils import prep_logger
import addresses

import snapshot_metrics


logger = logging.getLogger("snapshots")
prep_logger(logger, level=logging.INFO)


async def prepare_unspent_boxes_snapshot(conn: pg.Connection, height: int):
    """
    Prepare unspent boxes snapshot table for given height.
    """
    logger.info(f"Preparing unspent boxes snapshot for block {height}")

    qry = dedent(
        f"""
        create table mtr.unspent_boxes_snapshot as
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
        "alter table mtr.unspent_boxes_snapshot add primary key (box_id);"
    )


async def prepare_address_balances_snapshot(conn: pg.Connection):
    """
    Prepare address balances snapshot.

    Relies on uspent boxes snapshot.
    """
    logger.info(f"Preparing address balances snapshot")

    qry = dedent(
        """
        create table mtr.address_balances_snapshot as
            select nos.address
                , left(nos.address, 1) = '9' and length(nos.address) = 51 as p2pk
                , sum(nos.value) as value
            from mtr.unspent_boxes_snapshot ubs
            join node_outputs nos on nos.box_id = ubs.box_id
            group by 1, 2;
        """
    )

    await conn.execute(qry)
    await conn.execute(
        "alter table mtr.address_balances_snapshot add primary key (address);"
    )
    await conn.execute("create index on mtr.address_balances_snapshot (value);")


async def drop_snapshots(conn: pg.Connection):
    """
    Drop all snapshot tables.
    """
    await conn.execute("drop table mtr.unspent_boxes_snapshot;")
    await conn.execute("drop table mtr.address_balances_snapshot;")


async def qry_unprocessed_first_of_day_block_heights(conn: pg.Connection) -> List[int]:
    """
    Returns list of heights corresponding to unprocessed first-of-day blocks.

    Check for unprocessed blocks across all snapshot metrics tables.
    """
    tables = [
        "mtr.address_counts_by_minimal_balance",
        "mtr.contract_counts_by_minimal_balance",
        "mtr.top_addresses_supply",
        "mtr.top_contracts_supply",
        "mtr.cexs_supply",
    ]

    # Retrieves array of unprocessed heights for given table
    qry = dedent(
        """
        with last_processed_day as (
            select 0 as timestamp -- ensure at least one row when starting from scratch
            union
            select timestamp
            from {0}
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
        left join {0} tbl
            on tbl.timestamp = fdb.timestamp
        where tbl.timestamp is null
        order by 1;
        """
    )

    # Find unprocessed heights across all tables
    all_heights = set()
    for tbl in tables:
        r = await conn.fetchrow(qry.format(tbl))
        heights = r[0] if r[0] is not None else []
        all_heights = all_heights.union(heights)

    # Sort
    heights = list(heights)
    heights.sort()

    return heights


async def qry_current_block(conn: pg.Connection) -> int:
    """
    Returns height of latest block
    """
    return (
        await conn.fetchrow("select height from node_headers order by 1 desc limit 1;")
    )[0]


async def qry_block_timestamp(conn: pg.Connection, height: int) -> int:
    """
    Get timestamp for given height.
    """
    qry = f"select timestamp from node_headers where main_chain and height = {height};"
    r = await conn.fetchrow(qry)
    return r[0]


async def sync(conn: pg.Connection):
    """
    Main sync function.
    """
    logger.info("Looking for unprocessed first-of-day blocks")

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

            await snapshot_metrics.addresses.sync(conn, h, timestamp)
            await snapshot_metrics.contracts.sync(conn, h, timestamp)
            await snapshot_metrics.distribution.sync(conn, h, timestamp)
            await snapshot_metrics.tvl.sync(conn, h, timestamp)
            await snapshot_metrics.cexs.sync(conn, h, timestamp)
            await snapshot_metrics.utxos.sync(conn, h, timestamp)

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
