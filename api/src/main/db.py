import os
import asyncpg
import json


CONNECTION_POOL = None


async def init_connection_pool():
    global CONNECTION_POOL
    dbstr = f"postgresql://{os.environ['POSTGRES_PASSWORD']}:ergo@db/ergo"
    CONNECTION_POOL = await asyncpg.create_pool(dbstr)


async def get_latest_block_height():
    qry = "SELECT MAX(height) AS height FROM node_headers;"
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return row["height"]


async def get_latest_sync_height():
    qry = "select last_sync_height as height from ew.sync_status;"
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return row["height"]


async def get_oracle_pools_ergusd_oracle_stats():
    """
    ERG/USD oracle stats
    """
    qry = """
        SELECT oracle_id
            , address
            , commits
            , accepted_commits
            , collections
            , first_commit
            , last_commit
            , last_accepted
            , last_collection
        FROM ew.oracle_pools_ergusd_oracle_stats_mv;
    """
    async with CONNECTION_POOL.acquire() as conn:
        rows = await conn.fetch(qry)
    return [dict(r) for r in rows]
