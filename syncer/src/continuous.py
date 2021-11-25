"""
Maintains running stats by injesting each block.
"""
from datetime import datetime
from textwrap import dedent
from typing import NamedTuple, List
import logging
import asyncio
from decimal import Decimal

import asyncpg as pg

from utils import prep_logger
import addresses
from ergo import circ_supply

logger = logging.getLogger("continuous")
prep_logger(logger, level=logging.INFO)


class BlockStats(NamedTuple):
    height: int
    circ_supply: int  # nano
    transferred_value: int  # nano
    age: int  # milliseconds
    transactions: int


def emission(height: int) -> int:
    """
    Emission rate (nanoERG/block) at given height.
    """
    initial_rate = 75
    fixed_rate_period = 525600
    epoch_length = 64800
    step = 3

    if height <= fixed_rate_period:
        em = initial_rate
    else:
        em = initial_rate - (((height - fixed_rate_period) // epoch_length) + 1) * step
    return em * 10 ** 9


async def qry_milliseconds_since_previous_block(
    conn: pg.Connection, height: int
) -> int:
    """
    Query returning seconds between timestamps of given and previous blocks.
    """
    qry = dedent(
        f"""
        select (timestamp - lag(timestamp) over (order by height))
        from node_headers
        where main_chain
            and height = {height} or height = {height} - 1
        order by height desc limit 1;
        """
    )
    r = await conn.fetchrow(qry)
    return r[0]


async def qry_block_transferred_value(conn: pg.Connection, height: int) -> int:
    """
    Calculate transferred value within a block.
    Transferred value is nanoerg transfered to different address, excluding coinbase emissions.

    With miners mining their own transactions, the tx fees can end back into emitting address.
    See block 3355 for an example.
    Ideally those tx fees would not be counted as "transferred value". This is ignored here.
    """
    qry = dedent(
        f"""
        with transactions as (
            select inclusion_height as height, id
            from node_transactions
            where main_chain
                and inclusion_height = {height}
        ), inputs as (
            select txs.id as tx_id
                , nos.address
                , sum(nos.value) as value
            from transactions txs
            join node_inputs nis on nis.tx_id = txs.id
            join node_outputs nos on nos.box_id = nis.box_id
            where nis.main_chain and nos.main_chain
                -- exclude coinbase emission txs
                and nos.address <> '{addresses.coinbase}'
                -- exclude miner fees reward txs as already included in other txs
                -- see 3 txs of block 3357 for an example
                and nos.address <> '{addresses.tx_fees}'
            group by 1, 2
        ), outputs as (
            select txs.id as tx_id
                , nos.address
                , sum(nos.value) as value
            from transactions txs
            join node_outputs nos on nos.tx_id = txs.id
            where nos.main_chain
            group by 1, 2
        )
        select coalesce(sum(i.value - coalesce(o.value, 0)), 0) as value
        from inputs i
        left join outputs o
            on o.address = i.address
            and o.tx_id = i.tx_id;
        """
    )
    r = await conn.fetchrow(qry)
    return r[0]


async def qry_mean_age_at_block(conn: pg.Connection, height: int) -> int:
    """
    Retrieve mean age of supply for given block.
    """
    qry = dedent(
        f"""
        select mean_age_ms
        from con.block_stats
        where height = {height};
        """
    )
    r = await conn.fetchrow(qry)
    return r[0]


async def qry_block_transactions(conn: pg.Connection, height: int) -> int:
    """
    Return number of transactions in block
    """
    qry = dedent(
        """
        select count(*)
        from node_transactions
        where inclusion_height = $1;
        """
    )
    r = await conn.fetchrow(qry, height)
    return r[0]


def calc_supply_age(prev_cs, cs, transferred_value, prev_age_ms, ms_since_prev_block) -> int:
    """
    Calculate mean supply height in ms from given args.

    Definitions:
     - h: height
     - s(h): circulating supply at h
     - a(h): mean supply age at h
     - e(h): coinbase emission for block h
     - x(h): transferred ERG in block h, excluding r(h)
     - t(h): time between current and previous block

    At h = 1
    --------
    s(1) = e(1)
    a(1) = 0
    x(1) = 0

    At h = 2
    --------
    x(2) <= s(1)
    s(2) = s(1) + e(2)
    a(2) = [ (s(1) - x(2)) * (a(1) + t(h)) ] / s(2)

    At h = n
    --------
    x(n) <= s(n-1)
    s(n) = s(n-1) + e(n)
    a(n) = [ (s(n-1) - x(n)) * (a(n-1) + t(n)) ] / s(n)
    """
    return ((prev_cs - transferred_value) * (prev_age_ms + ms_since_prev_block)) / cs


async def qry_block_stats(conn: pg.Connection, height: int) -> BlockStats:
    """
    Return block stats for given height.
    """
    prev_cs = circ_supply(height - 1) * 10 ** 9
    cs = circ_supply(height) * 10 ** 9
    transferred_value = await qry_block_transferred_value(conn, height)
    prev_age_ms = await qry_mean_age_at_block(conn, height - 1)
    ms_since_prev_block = await qry_milliseconds_since_previous_block(conn, height)

    age_ms = calc_supply_age(prev_cs, cs, transferred_value, prev_age_ms, ms_since_prev_block)

    nb_of_txs = await qry_block_transactions(conn, height)

    return BlockStats(height, cs, transferred_value, age_ms, nb_of_txs)


async def qry_last_processed_block(conn: pg.Connection) -> int:
    """
    Returns height of last processed block.
    """
    return (
        await conn.fetchrow(
            "select height from con.block_stats order by 1 desc limit 1;"
        )
    )[0]


async def qry_current_block(conn: pg.Connection) -> int:
    """
    Returns height of latest block
    """
    return (
        await conn.fetchrow("select height from node_headers order by 1 desc limit 1;")
    )[0]


async def insert_block_stats(conn: pg.Connection, row: BlockStats):
    """
    Add a row to the block stats table.
    """
    logger.info(f"Inserting block stats for block {row.height}")
    qry = dedent(
        """
        insert into con.block_stats (height, circulating_supply, transferred_value, mean_age_ms, transactions)
        values ($1, $2, $3, $4, $5)
        """
    )
    await conn.execute(qry, *row)


async def rollback_to_height(conn: pg.Connection, height: int):
    """
    Delete block stats for blocks above given height.
    """
    logger.info(f"Rolling back to height: {height}")
    qry = dedent(
        f"""
        delete from con.block_stats
        where height > {height};
        """
    )
    await conn.execute(qry)


async def refresh_age_series(conn: pg.Connection):
    logger.info("Refreshing age series")

    async with conn.transaction():
        await conn.execute("truncate table con.mean_age_series_daily;");

        qry = dedent(
            """
            insert into con.mean_age_series_daily(timestamp, mean_age_days)
                with fod_timestamps as (
                    select min(timestamp) as timestamp
                        , min(height) as height
                    from node_headers
                    where main_chain
                    group by timestamp / 86400000
                )
                select t.timestamp
                    , s.mean_age_ms / 86400000.
                from con.block_stats s
                join fod_timestamps t on t.height = s.height
                order by 1;
            """
        )
        await conn.execute(qry)


async def refresh_aggregate_series(conn: pg.Connection):
    logger.info("Refreshing aggregate series")

    async with conn.transaction():
        await conn.execute("truncate table con.aggregate_series_daily;");

        qry = dedent(
            """
            insert into con.aggregate_series_daily(timestamp, transferred_value, transactions)
                select MAX(timestamp) as timestamp
                    , sum(s.transferred_value) as tval
                    , sum(s.transactions) as txs
                from con.block_stats s
                join node_headers h on h.height = s.height
                where h.main_chain
                group by timestamp / 86400000
                order by 1;
            """
        )
        await conn.execute(qry)


async def sync(conn: pg.Connection):
    """
    Main sync function.
    """
    logger.info("Syncing started")
    last_processed = await qry_last_processed_block(conn)
    current_height = await qry_current_block(conn)

    # Rollback last 10 blocks to have been processed.
    # This is to account for any changes that may have occured in
    # low confirmation blocks.
    min_confirmations = 10
    last_processed = max(1, last_processed - min_confirmations)
    await rollback_to_height(conn, last_processed)

    heights_to_process = range(last_processed + 1, current_height + 1)
    logger.info(f"Number of blocks to process: {len(heights_to_process)}")

    for h in heights_to_process:
        stats = await qry_block_stats(conn, h)
        await insert_block_stats(conn, stats)

    await refresh_age_series(conn)
    await refresh_aggregate_series(conn)

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


