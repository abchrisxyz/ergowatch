from datetime import datetime
from textwrap import dedent
from typing import NamedTuple
import logging

import asyncpg as pg

from ergo import circ_supply

logger = logging.getLogger("snapshots")

METRIC_ID = "distribution"
SERIES_TABLE = "mtr.top_addresses_supply"
SUMMARY_TABLE = "mtr.top_addresses_supply_change_summary"

class Record(NamedTuple):
    timestamp: int
    top10: int
    top100: int
    top1k: int
    top10k: int
    total: int
    circulating_supply: int


async def block_is_processed(conn: pg.Connection, timestamp):
    """
    Returns True if block not included yet.
    """
    qry = dedent(
        f"""
        select timestamp
        from {SERIES_TABLE}
        where timestamp = $1;
        """
    )
    r = await conn.fetchrow(qry, timestamp)
    return r is not None


async def query(conn: pg.Connection, timestamp: int, height: int) -> Record:
    """
    Return current supply held in top x P2PK addreses, excluding known CEX's.

    Assumes snapshots are up to date.
    """

    qry = dedent(
        f"""
        with address_balances as (
            select bal.address
                , sum(value) as nano
            from mtr.address_balances_snapshot bal
            left join mtr.cex_addresses cex on cex.address = bal.address
            where bal.p2pk
                and cex.address is null
            group by 1
        ), ranked_addresses as (
                select row_number() over (order by nano desc) as value_rank
                    , sum(nano) over (order by nano desc rows between unbounded preceding and current row) as nano
                from address_balances bal
                order by nano desc
        )
        select
            (select nano from ranked_addresses where value_rank = 10) as t10
            ,(select nano from ranked_addresses where value_rank = 100) as t100
            ,(select nano from ranked_addresses where value_rank = 1000) as t1k
            ,(select nano from ranked_addresses where value_rank = 10000) as t10k
            ,(select sum(nano) from address_balances) as total
        ;
        """
    )

    r = await conn.fetchrow(qry)
    cs = circ_supply(height, nano=True)
    return Record(timestamp, *r, cs)


async def insert(conn: pg.Connection, row: Record):
    qry = dedent(
        f"""
        insert into {SERIES_TABLE} (
            timestamp,
            top10,
            top100,
            top1k,
            top10k,
            total,
            circulating_supply
        )
        values ($1, $2, $3, $4, $5, $6, $7);
        """
    )
    await conn.execute(qry, *row)


async def update_series(conn: pg.Connection, timestamp: int, height: int):
    logger.info(f"Updating {METRIC_ID} series")
    row = await query(conn, timestamp, height)
    await insert(conn, row)


async def refresh_change_summary(conn: pg.Connection):
    logger.info(f"Updating {METRIC_ID} change summary")

    await conn.execute(f"truncate {SUMMARY_TABLE};")

    template = dedent(
        f"""
        insert into {SUMMARY_TABLE} (
            col, latest, diff_1d, diff_1w, diff_4w, diff_6m, diff_1y
        )
            select '{{0}}' as col
                , {{0}} as latest
                , {{0}} - lead({{0}}, 1) over (order by timestamp desc) as diff_1d
                , {{0}} - lead({{0}}, 7) over (order by timestamp desc) as diff_7d
                , {{0}} - lead({{0}}, 28) over (order by timestamp desc) as diff_4w
                , {{0}} - lead({{0}}, 183) over (order by timestamp desc) as diff_6m
                , {{0}} - lead({{0}}, 365) over (order by timestamp desc) as diff_1y
            from {SERIES_TABLE}
            order by timestamp desc
            limit 1;
        """
    )

    columns = [
        "top10",
        "top100",
        "top1k",
        "top10k",
        "total",
        "circulating_supply",
    ]
    for col in columns:
        qry = template.format(col)
        await conn.execute(qry)


async def sync(conn: pg.Connection, height: int, timestamp: int):
    """
    Main sync function.
    """
    if await block_is_processed(conn, timestamp):
        logger.info(f"Block already processed for {METRIC_ID}")
        return

    await update_series(conn, timestamp, height)
    await refresh_change_summary(conn)
