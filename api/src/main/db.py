import os
import asyncpg


DBSTR = f"postgresql://{os.environ['POSTGRES_PASSWORD']}:ergo@ergo-postgresql/ergo"


async def get_latest_block_height():
    qry = "SELECT MAX(height) AS height FROM node_headers;"
    conn = await asyncpg.connect(DBSTR)
    row = await conn.fetchrow(qry)
    await conn.close()
    return row['height']
