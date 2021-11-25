from datetime import datetime
from textwrap import dedent
from typing import NamedTuple
import logging

import asyncpg as pg


logger = logging.getLogger("snapshots")

METRIC_ID = "utxos"
SERIES_TABLE = "mtr.unspent_boxes"
SUMMARY_TABLE = "mtr.unspent_boxes_change_summary"
LIST_TABLE = "mtr.unspent_boxes_top_addresses"


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


async def update_series(conn: pg.Connection, timestamp: int):
    logger.info(f"Updating {METRIC_ID} series")

    qry = dedent(
        f"""
        insert into {SERIES_TABLE} (timestamp, boxes)
        select $1, count(*)
        from mtr.unspent_boxes_snapshot;
        """
    )

    await conn.execute(qry, timestamp)


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
        "boxes",
    ]
    for col in columns:
        qry = template.format(col)
        await conn.execute(qry)


async def refresh_top_addresses_list(conn: pg.Connection):
    """
    Referesh list of addresses with most boxes
    """
    logger.info(f"Updating {METRIC_ID} top list")

    await conn.execute(f"truncate {LIST_TABLE};")

    template = dedent(
        f"""
        insert into {LIST_TABLE} (address, boxes)
            select nos.address
                , count(*)
            from mtr.unspent_boxes_snapshot ubs
            join node_outputs nos on nos.box_id = ubs.box_id
            group by 1
            order by 2 desc
            limit 1000;
        """
    )

    columns = [
        "boxes",
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
    await refresh_change_summary(conn)
    await refresh_top_addresses_list(conn)
