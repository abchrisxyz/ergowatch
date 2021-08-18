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
        with state as (
            select  (1 / oracle_price * 1000000000)::integer as peg_rate_nano
                , (reserves * 1000000000)::bigint as reserves
                , (circ_sigusd * 100)::integer as circ_sigusd
                , circ_sigrsv
                , net_sc_erg
                , net_rc_erg
            from ew.sigmausd_series_history_mv
            order by timestamp desc limit 1
        ), cumulative as (
            select cum_usd_erg_in as cum_sc_erg_in
                , cum_rsv_erg_in as cum_rc_erg_in
            from ew.sigmausd_history_transactions_cumulative
            order by bank_box_idx desc limit 1
        )
        select *
        from state, cumulative;
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


async def get_sigmausd_history(days: int):
    """
    Last *days* of bank history.
    """
    qry = f"""
        select array_agg(timestamp order by timestamp) as timestamps
            , array_agg(oracle_price order by timestamp) as oracle_prices
            , array_agg(reserves order by timestamp) as reserves
            , array_agg(circ_sigusd order by timestamp) as circ_sigusd
            , array_agg(circ_sigrsv order by timestamp) as circ_sigrsv
        from ew.sigmausd_series_history_mv
        where timestamp >= (extract(epoch from now() - '{days} days'::interval))::bigint;
    """
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return dict(row)


async def get_sigmausd_history_full():
    """
    Full bank history.
    """
    qry = f"""
        select array_agg(timestamp order by timestamp) as timestamps
            , array_agg(oracle_price order by timestamp) as oracle_prices
            , array_agg(reserves order by timestamp) as reserves
            , array_agg(circ_sigusd order by timestamp) as circ_sigusd
            , array_agg(circ_sigrsv order by timestamp) as circ_sigrsv
        from ew.sigmausd_series_history_mv;
    """
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return dict(row)
