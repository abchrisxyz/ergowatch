from datetime import datetime
from textwrap import dedent
from typing import NamedTuple
import logging

import asyncpg as pg

from ergo import circ_supply

logger = logging.getLogger("snapshots")

METRIC_ID = "cexs"
DETAIL_TABLE = "mtr.cex_addresses_supply"
SERIES_TABLE = "mtr.cexs_supply"
SUMMARY_TABLE = "mtr.cexs_supply_change_summary"


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


async def update_detail(conn: pg.Connection, timestamp: int):
    logger.info(f"Updating {METRIC_ID} detail")
    qry = dedent(
        f"""
        insert into {DETAIL_TABLE} (timestamp, address, nano)
            select $1::bigint as timestamp
                , cex.address
                , sum(value) as nano
            from mtr.address_balances_snapshot bal
            join mtr.cex_addresses cex on cex.address = bal.address
            group by 1, 2
            order by 1, 2;
        """
    )
    await conn.execute(qry, timestamp)


async def update_series(conn: pg.Connection, timestamp: int, height: int):
    logger.info(f"Updating {METRIC_ID} series")
    qry = dedent(
        f"""
        insert into {SERIES_TABLE} (
            timestamp,
            circulating_supply,
            total,
            coinex,
            gateio,
            kucoin
        )
            select $1::bigint as timestamp
                , $2::bigint as circulating_supply
                , sum(nano) as total
                , sum(nano) filter (where cex.cex = 'coinex')
                , sum(nano) filter (where cex.cex = 'gate')
                , sum(nano) filter (where cex.cex = 'kucoin')
            from {DETAIL_TABLE} det
            join mtr.cex_addresses cex on cex.address = det.address
            where det.timestamp = $1
            group by 1, 2
            order by 1, 2;
        """
    )
    cs = circ_supply(height, nano=True)
    await conn.execute(qry, timestamp, cs)


async def refresh_change_summary(conn: pg.Connection):
    logger.info(f"Updating {METRIC_ID} change summary")

    await conn.execute(f"truncate {SUMMARY_TABLE};")

    template = dedent(
        f"""
        insert into {SUMMARY_TABLE} (
            col, latest, diff_1d, diff_1w, diff_4w, diff_6m, diff_1y
        )
            select '{{0}}' as col
                , {{0}} / 10^9 as latest
                , ({{0}} - lead({{0}}, 1) over (order by timestamp desc)) / 10^9 as diff_1d
                , ({{0}} - lead({{0}}, 7) over (order by timestamp desc)) / 10^9 as diff_7d
                , ({{0}} - lead({{0}}, 28) over (order by timestamp desc)) / 10^9 as diff_4w
                , ({{0}} - lead({{0}}, 183) over (order by timestamp desc)) / 10^9 as diff_6m
                , ({{0}} - lead({{0}}, 365) over (order by timestamp desc)) / 10^9 as diff_1y
            from {SERIES_TABLE}
            order by timestamp desc
            limit 1;
        """
    )

    cexs = [
        "circulating_supply",
        "total",
        "coinex",
        "gateio",
        "kucoin",
    ]
    for cex in cexs:
        qry = template.format(cex)
        await conn.execute(qry)

    # TODO: refactor relative changes into own table
    template = dedent(
        f"""
        insert into {SUMMARY_TABLE} (
            col, latest, diff_1d, diff_1w, diff_4w, diff_6m, diff_1y
        )
            select '{{0}}_rel' as col
                , {{0}}::numeric / circulating_supply * 100 as latest
                , ({{0}}::numeric / circulating_supply - lead({{0}}::numeric / circulating_supply, 1) over (order by timestamp desc)) * 100 as diff_1d
                , ({{0}}::numeric / circulating_supply - lead({{0}}::numeric / circulating_supply, 7) over (order by timestamp desc)) * 100 as diff_7d
                , ({{0}}::numeric / circulating_supply - lead({{0}}::numeric / circulating_supply, 28) over (order by timestamp desc)) * 100 as diff_4w
                , ({{0}}::numeric / circulating_supply - lead({{0}}::numeric / circulating_supply, 183) over (order by timestamp desc)) * 100 as diff_6m
                , ({{0}}::numeric / circulating_supply - lead({{0}}::numeric / circulating_supply, 365) over (order by timestamp desc)) * 100 as diff_1y
            from {SERIES_TABLE}
            order by timestamp desc
            limit 1;
        """
    )

    cexs = [
        "total",
        "coinex",
        "gateio",
        "kucoin",
    ]
    for cex in cexs:
        qry = template.format(cex)
        await conn.execute(qry)


async def sync(conn: pg.Connection, height: int, timestamp: int):
    """
    Main sync function.
    """
    if await block_is_processed(conn, timestamp):
        logger.info("Already processed - skipping block")
        return

    await update_detail(conn, timestamp)
    await update_series(conn, timestamp, height)
    await refresh_change_summary(conn)
