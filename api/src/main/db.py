import os
import asyncpg
import json

DBSTR = f"postgresql://{os.environ['POSTGRES_PASSWORD']}:ergo@ergo-postgresql/ergo"


async def get_latest_block_height():
    qry = "SELECT MAX(height) AS height FROM node_headers;"
    conn = await asyncpg.connect(DBSTR)
    row = await conn.fetchrow(qry)
    await conn.close()
    return row['height']


async def get_oracle_pool_commits(oracle_pool_id):
    """
    Nb of datapoint boxes by oracle addresses (from data-point boxes R4)
    """
    qry = """
        WITH counts AS (
            SELECT nos.additional_registers #>> '{R4,renderedValue}' AS oracle_address_hash
                , COUNT(*) -1 nb_of_commit_txs -- -1 to account for forging tx
            FROM node_outputs nos
            WHERE nos.address = (SELECT datapoint_address FROM ew.oracle_pools WHERE id = $1)
            GROUP BY 1
        )
        SELECT ahs.address
            , cnt.nb_of_commit_txs
        FROM counts cnt
        JOIN ew.oracle_pools_oracle_address_hashes ahs ON ahs.hash = cnt.oracle_address_hash
    """
    conn = await asyncpg.connect(DBSTR)
    rows = await conn.fetch(qry, oracle_pool_id)
    await conn.close()
    return {r['address']: r['nb_of_commit_txs'] for r in rows}


async def get_oracle_pool_commit_stats_ergusd():
    """
    ERG/USD commit stats
    """
    qry = """
        SELECT address
            , commits
            , accepted_commits
            , first_commit
            , last_commit
            , last_accepted
        FROM ew.oracle_pools_commit_stats_ergusd_mv;   
    """
    conn = await asyncpg.connect(DBSTR)
    rows = await conn.fetch(qry)
    await conn.close()
    return [dict(r) for r in rows]
