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


async def get_oracle_pools_ergusd_latest():
    """
    Latest ERG/USD oracle pool posting
    """
    qry = """
        select height
            , price
            , datapoints
        from ew.oracle_pools_ergusd_latest_posting_mv;
    """
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return dict(row)


async def get_oracle_pools_ergusd_recent_epoch_durations():
    qry = """
        select height as h
            , blocks as n
        from ew.oracle_pools_ergusd_recent_epoch_durations_mv
        order by 1;
    """
    async with CONNECTION_POOL.acquire() as conn:
        rows = await conn.fetch(qry)
    return [dict(r) for r in rows]


async def get_oracle_pools_ergusd_oracle_stats():
    """
    ERG/USD oracle stats
    """
    qry = """
        select oracle_id
            , address
            , commits
            , accepted_commits
            , collections
            , first_commit
            , last_commit
            , last_accepted
            , last_collection
        from ew.oracle_pools_ergusd_oracle_stats_mv
        order by 1;
    """
    async with CONNECTION_POOL.acquire() as conn:
        rows = await conn.fetch(qry)
    return [dict(r) for r in rows]


async def get_sigmausd_state():
    """
    Latest SigmaUSD bank state
    """
    qry = """
        with oracle as (
            select datapoint as peg_rate_nano
            from ew.oracle_pools_ergusd_prep_txs
            order by inclusion_height desc
            limit 1
        ), bank as (
            select nos.value as reserves
                , (nos.additional_registers #>> '{R4,renderedValue}')::numeric as circ_sigusd
                , (nos.additional_registers #>> '{R5,renderedValue}')::bigint as circ_sigrsv
            from ew.sigmausd_bank_boxes bbx
            join node_outputs nos on nos.box_id = bbx.box_id
            order by bbx.idx desc limit 1
        )
        select reserves
            , circ_sigusd
            , circ_sigrsv
            , peg_rate_nano
        from bank, oracle;
    """
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return dict(row)


async def get_sigmausd_sigrsv_ohlc_d():
    """
    SigRSV daily open high low close series.
    """
    qry = """
        select date as time
            , o as open
            , h as high
            , l as low
            , c as close
        from ew.sigmausd_sigrsv_ohlc_d_mv
        order by 1;
    """
    async with CONNECTION_POOL.acquire() as conn:
        rows = await conn.fetch(qry)
    return [dict(r) for r in rows]


async def get_sigmausd_net_sigusd_flow():
    """
    Net ERG flow resulting from SigUSD transactions
    """
    qry = """
        select timestamp as t, net_usd_erg as v
        from ew.sigmausd_sigusd_net_flow_mv;
    """
    async with CONNECTION_POOL.acquire() as conn:
        rows = await conn.fetch(qry)
    return [dict(r) for r in rows]


async def get_sigmausd_net_sigrsv_flow():
    """
    Net ERG flow resulting from SigRSV transactions
    """
    qry = """
        select timestamp as t, net_rsv_erg as v
        from ew.sigmausd_sigrsv_net_flow_mv;
    """
    async with CONNECTION_POOL.acquire() as conn:
        rows = await conn.fetch(qry)
    return [dict(r) for r in rows]


async def get_sigmausd_liabilities():
    """
    SigmaUSD liabilities over time.
    """
    qry = """
        select timestamp as t, liabs as v
        from ew.sigmausd_liabs_mv;
    """
    async with CONNECTION_POOL.acquire() as conn:
        rows = await conn.fetch(qry)
    return [dict(r) for r in rows]
