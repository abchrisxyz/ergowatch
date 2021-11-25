from datetime import datetime
from textwrap import dedent
from typing import NamedTuple, List
import logging

import asyncpg as pg

from ergo import circ_supply

logger = logging.getLogger("snapshots")

METRIC_ID = "contracts"
SERIES_TABLE = "mtr.contract_counts_by_minimal_balance"
SUMMARY_TABLE = "mtr.contract_counts_by_minimal_balance_change_summary"


class Record(NamedTuple):
    timestamp: int
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


async def query(conn: pg.Connection, timestamp) -> Record:
    """
    Returns current P2S(H) address counts by minimal balance.

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
        from mtr.address_balances_snapshot
        where not p2pk
        ;
        """
    )

    r = await conn.fetchrow(qry)
    return Record(timestamp, *r)


async def insert(conn: pg.Connection, row: Record):
    qry = dedent(
        f"""
        insert into {SERIES_TABLE} (
            timestamp,
            total,
            gte_0_001,
            gte_0_01,
            gte_0_1,
            gte_1,
            gte_10,
            gte_100,
            gte_1k,
            gte_10k,
            gte_100k,
            gte_1m
        )
        values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);
        """
    )
    await conn.execute(qry, *row)


async def update_series(conn: pg.Connection, timestamp: int):
    logger.info(f"Updating {METRIC_ID} series")
    row = await query(conn, timestamp)
    await insert(conn, row)


async def refresh_change_summary(conn: pg.Connection, height: int):
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
        "total",
        "gte_0_001",
        "gte_0_01",
        "gte_0_1",
        "gte_1",
        "gte_10",
        "gte_100",
        "gte_1k",
        "gte_10k",
        "gte_100k",
        "gte_1m",
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

    await update_series(conn, timestamp)
    await refresh_change_summary(conn, height)
