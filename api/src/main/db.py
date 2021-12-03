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
        from orp.ergusd_latest_posting_mv;
    """
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return dict(row)


async def get_oracle_pools_ergusd_recent_epoch_durations():
    qry = """
        select height as h
            , blocks as n
        from orp.ergusd_recent_epoch_durations_mv
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
        from orp.ergusd_oracle_stats_mv
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
            from sig.series_history_mv
            order by timestamp desc limit 1
        ), cumulative as (
            select cum_usd_erg_in as cum_sc_erg_in
                , cum_rsv_erg_in as cum_rc_erg_in
            from sig.history_transactions_cumulative
            order by bank_box_idx desc limit 1
        )
        select peg_rate_nano
            , reserves
            , circ_sigusd
            , circ_sigrsv
            , net_sc_erg
            , net_rc_erg
            , cum_sc_erg_in
            , cum_rc_erg_in
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
        from sig.sigrsv_ohlc_d_mv
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
        from sig.series_history_mv
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
        from sig.series_history_mv;
    """
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return dict(row)


async def get_metrics_preview():
    """
    Summary stats for metrics landing page.
    """
    qry = """
        select m.total_addresses
            , m.total_contracts
            , m.top100_supply_fraction
            , m.contracts_supply_fraction
            , m.cexs_supply_fraction
            , m.boxes as utxos
            , c.mean_age_days
            , c.transferred_value_24h
            , c.transactions_24h
        from mtr.preview m, con.preview c;
    """
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return dict(row)


async def get_metrics_address_counts_summary():
    """
    Latest summary of address counts.
    """
    qry = f"""
        select col
            , latest
            , diff_1d
            , diff_1w
            , diff_4w
            , diff_6m
            , diff_1y
        from mtr.address_counts_by_minimal_balance_change_summary;
    """
    async with CONNECTION_POOL.acquire() as conn:
        rows = await conn.fetch(qry)
    return [dict(r) for r in rows]


async def get_metrics_addresses_series(days: int):
    """
    Last *days* days of addresses series.
    """
    qry = f"""
        select array_agg(mtr.timestamp / 1000 order by mtr.timestamp) as timestamps
            , array_agg(round(cgo.usd, 2) order by mtr.timestamp) as ergusd
            , array_agg(total order by mtr.timestamp) as total
            , array_agg(gte_0_001 order by mtr.timestamp) as gte_0_001
            , array_agg(gte_0_01 order by mtr.timestamp) as gte_0_01
            , array_agg(gte_0_1 order by mtr.timestamp) as gte_0_1
            , array_agg(gte_1 order by mtr.timestamp) as gte_1
            , array_agg(gte_10 order by mtr.timestamp) as gte_10
            , array_agg(gte_100 order by mtr.timestamp) as gte_100
            , array_agg(gte_1k order by mtr.timestamp) as gte_1k
            , array_agg(gte_10k order by mtr.timestamp) as gte_10k
            , array_agg(gte_100k order by mtr.timestamp) as gte_100k
            , array_agg(gte_1m order by mtr.timestamp) as gte_1m
        from mtr.address_counts_by_minimal_balance mtr
        left join cgo.price_at_first_of_day_block cgo
            on cgo.timestamp = mtr.timestamp
        where mtr.timestamp > (
            select timestamp
            from mtr.address_counts_by_minimal_balance
            order by 1 desc
            limit 1 offset {days}
        );
    """
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return dict(row)


async def get_metrics_addresses_series_full():
    """
    Full addresses series.
    """
    qry = f"""
        select array_agg(mtr.timestamp / 1000 order by mtr.timestamp) as timestamps
            , array_agg(round(cgo.usd, 2) order by mtr.timestamp) as ergusd
            , array_agg(total order by mtr.timestamp) as total
            , array_agg(gte_0_001 order by mtr.timestamp) as gte_0_001
            , array_agg(gte_0_01 order by mtr.timestamp) as gte_0_01
            , array_agg(gte_0_1 order by mtr.timestamp) as gte_0_1
            , array_agg(gte_1 order by mtr.timestamp) as gte_1
            , array_agg(gte_10 order by mtr.timestamp) as gte_10
            , array_agg(gte_100 order by mtr.timestamp) as gte_100
            , array_agg(gte_1k order by mtr.timestamp) as gte_1k
            , array_agg(gte_10k order by mtr.timestamp) as gte_10k
            , array_agg(gte_100k order by mtr.timestamp) as gte_100k
            , array_agg(gte_1m order by mtr.timestamp) as gte_1m
        from mtr.address_counts_by_minimal_balance mtr
        left join cgo.price_at_first_of_day_block cgo
            on cgo.timestamp = mtr.timestamp
        ;
    """
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return dict(row)


async def get_metrics_contract_counts_summary():
    """
    Latest summary of contract counts.
    """
    qry = f"""
        select col
            , latest
            , diff_1d
            , diff_1w
            , diff_4w
            , diff_6m
            , diff_1y
        from mtr.contract_counts_by_minimal_balance_change_summary;
    """
    async with CONNECTION_POOL.acquire() as conn:
        rows = await conn.fetch(qry)
    return [dict(r) for r in rows]


async def get_metrics_contracts_series(days: int):
    """
    Last *days* days of contracts series.
    """
    qry = f"""
        select array_agg(mtr.timestamp / 1000 order by mtr.timestamp) as timestamps
            , array_agg(round(cgo.usd, 2) order by mtr.timestamp) as ergusd
            , array_agg(total order by mtr.timestamp) as total
            , array_agg(gte_0_001 order by mtr.timestamp) as gte_0_001
            , array_agg(gte_0_01 order by mtr.timestamp) as gte_0_01
            , array_agg(gte_0_1 order by mtr.timestamp) as gte_0_1
            , array_agg(gte_1 order by mtr.timestamp) as gte_1
            , array_agg(gte_10 order by mtr.timestamp) as gte_10
            , array_agg(gte_100 order by mtr.timestamp) as gte_100
            , array_agg(gte_1k order by mtr.timestamp) as gte_1k
            , array_agg(gte_10k order by mtr.timestamp) as gte_10k
            , array_agg(gte_100k order by mtr.timestamp) as gte_100k
            , array_agg(gte_1m order by mtr.timestamp) as gte_1m
        from mtr.contract_counts_by_minimal_balance mtr
        left join cgo.price_at_first_of_day_block cgo
            on cgo.timestamp = mtr.timestamp
        where mtr.timestamp > (
            select timestamp
            from mtr.contract_counts_by_minimal_balance
            order by 1 desc
            limit 1 offset {days}
        );
    """
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return dict(row)


async def get_metrics_contracts_series_full():
    """
    Full contracts series.
    """
    qry = f"""
        select array_agg(mtr.timestamp / 1000 order by mtr.timestamp) as timestamps
            , array_agg(round(cgo.usd, 2) order by mtr.timestamp) as ergusd
            , array_agg(total order by mtr.timestamp) as total
            , array_agg(gte_0_001 order by mtr.timestamp) as gte_0_001
            , array_agg(gte_0_01 order by mtr.timestamp) as gte_0_01
            , array_agg(gte_0_1 order by mtr.timestamp) as gte_0_1
            , array_agg(gte_1 order by mtr.timestamp) as gte_1
            , array_agg(gte_10 order by mtr.timestamp) as gte_10
            , array_agg(gte_100 order by mtr.timestamp) as gte_100
            , array_agg(gte_1k order by mtr.timestamp) as gte_1k
            , array_agg(gte_10k order by mtr.timestamp) as gte_10k
            , array_agg(gte_100k order by mtr.timestamp) as gte_100k
            , array_agg(gte_1m order by mtr.timestamp) as gte_1m
        from mtr.contract_counts_by_minimal_balance mtr
        left join cgo.price_at_first_of_day_block cgo
            on cgo.timestamp = mtr.timestamp
        ;
    """
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return dict(row)


async def get_metrics_distribution_summary():
    """
    Latest summary of top p2pk addresses supply.
    """
    qry = f"""
        select col
            , latest as latest
            , diff_1d as diff_1d
            , diff_1w as diff_1w
            , diff_4w as diff_4w
            , diff_6m as diff_6m
            , diff_1y as diff_1y
        from mtr.top_addresses_supply_change_summary;
    """
    async with CONNECTION_POOL.acquire() as conn:
        rows = await conn.fetch(qry)
    return [dict(r) for r in rows]


async def get_metrics_distribution_series(days: int):
    """
    Last *days* days of distribution series.
    """
    qry = f"""
        select array_agg(mtr.timestamp / 1000 order by mtr.timestamp) as timestamps
            , array_agg(round(cgo.usd, 2) order by mtr.timestamp) as ergusd
            , array_agg(top10 / 10^9 order by mtr.timestamp) as top10
            , array_agg(top100 / 10^9 order by mtr.timestamp) as top100
            , array_agg(top1k / 10^9 order by mtr.timestamp) as top1k
            --, array_agg(top10k / 10^9 order by mtr.timestamp) as top10k
            , array_agg(total / 10^9 order by mtr.timestamp) as total
            , array_agg(circulating_supply / 10^9 order by mtr.timestamp) as circulating_supply
        from mtr.top_addresses_supply mtr
        left join cgo.price_at_first_of_day_block cgo
            on cgo.timestamp = mtr.timestamp
        where mtr.timestamp > (
            select timestamp
            from mtr.top_addresses_supply
            order by 1 desc
            limit 1 offset {days}
        );
    """
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return dict(row)


async def get_metrics_distribution_series_full():
    """
    Full distribution series.
    """
    qry = f"""
        select array_agg(mtr.timestamp / 1000 order by mtr.timestamp) as timestamps
            , array_agg(round(cgo.usd, 2) order by mtr.timestamp) as ergusd
            , array_agg(top10  / 10^9 order by mtr.timestamp) as top10
            , array_agg(top100 / 10^9  order by mtr.timestamp) as top100
            , array_agg(top1k  / 10^9 order by mtr.timestamp) as top1k
            --, array_agg(top10k / 10^9  order by mtr.timestamp) as top10k
            , array_agg(total  / 10^9 order by mtr.timestamp) as total
            , array_agg(circulating_supply / 10^9 order by mtr.timestamp) as circulating_supply
        from mtr.top_addresses_supply mtr
        left join cgo.price_at_first_of_day_block cgo
            on cgo.timestamp = mtr.timestamp
        ;
    """
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return dict(row)


async def get_metrics_tvl_summary():
    """
    Latest summary of top contract addresses supply.
    """
    qry = f"""
        select col
            , latest as latest
            , diff_1d as diff_1d
            , diff_1w as diff_1w
            , diff_4w as diff_4w
            , diff_6m as diff_6m
            , diff_1y as diff_1y
        from mtr.top_contracts_supply_change_summary;
    """
    async with CONNECTION_POOL.acquire() as conn:
        rows = await conn.fetch(qry)
    return [dict(r) for r in rows]


async def get_metrics_tvl_series(days: int):
    """
    Last *days* days of tvl series.
    """
    qry = f"""
        select array_agg(mtr.timestamp / 1000 order by mtr.timestamp) as timestamps
            , array_agg(round(cgo.usd, 2) order by mtr.timestamp) as ergusd
            , array_agg(top10 / 10^9 order by mtr.timestamp) as top10
            , array_agg(top100 / 10^9 order by mtr.timestamp) as top100
            , array_agg(top1k / 10^9 order by mtr.timestamp) as top1k
            --, array_agg(top10k / 10^9 order by mtr.timestamp) as top10k
            , array_agg(total / 10^9 order by mtr.timestamp) as total
            , array_agg(circulating_supply / 10^9 order by mtr.timestamp) as circulating_supply
        from mtr.top_contracts_supply mtr
        left join cgo.price_at_first_of_day_block cgo
            on cgo.timestamp = mtr.timestamp
        where mtr.timestamp > (
            select timestamp
            from mtr.top_contracts_supply
            order by 1 desc
            limit 1 offset {days}
        );
    """
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return dict(row)


async def get_metrics_tvl_series_full():
    """
    Full tvl series.
    """
    qry = f"""
        select array_agg(mtr.timestamp / 1000 order by mtr.timestamp) as timestamps
            , array_agg(round(cgo.usd, 2) order by mtr.timestamp) as ergusd
            , array_agg(top10  / 10^9 order by mtr.timestamp) as top10
            , array_agg(top100 / 10^9  order by mtr.timestamp) as top100
            , array_agg(top1k  / 10^9 order by mtr.timestamp) as top1k
            --, array_agg(top10k / 10^9  order by mtr.timestamp) as top10k
            , array_agg(total  / 10^9 order by mtr.timestamp) as total
            , array_agg(circulating_supply  / 10^9 order by mtr.timestamp) as circulating_supply
        from mtr.top_contracts_supply mtr
        left join cgo.price_at_first_of_day_block cgo
            on cgo.timestamp = mtr.timestamp
        ;
    """
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return dict(row)


async def get_metrics_cexs_summary():
    """
    Latest summary of cex addresses supply.
    """
    qry = f"""
        select col
            , latest as latest
            , diff_1d as diff_1d
            , diff_1w as diff_1w
            , diff_4w as diff_4w
            , diff_6m as diff_6m
            , diff_1y as diff_1y
        from mtr.cexs_supply_change_summary;
    """
    async with CONNECTION_POOL.acquire() as conn:
        rows = await conn.fetch(qry)
    return [dict(r) for r in rows]


async def get_metrics_cexs_series(days: int):
    """
    Last *days* days of cexs series.
    """
    qry = f"""
        select array_agg(mtr.timestamp / 1000 order by mtr.timestamp) as timestamps
            , array_agg(round(cgo.usd, 2) order by mtr.timestamp) as ergusd
            , array_agg(coinex  / 10^9 order by mtr.timestamp) as coinex
            , array_agg(gateio / 10^9  order by mtr.timestamp) as gateio
            , array_agg(kucoin / 10^9 order by mtr.timestamp) as kucoin
            , array_agg(total  / 10^9 order by mtr.timestamp) as total
            , array_agg(circulating_supply  / 10^9 order by mtr.timestamp) as circulating_supply
        from mtr.cexs_supply mtr
        left join cgo.price_at_first_of_day_block cgo
            on cgo.timestamp = mtr.timestamp
        where mtr.timestamp > (
            select timestamp
            from mtr.cexs_supply
            order by 1 desc
            limit 1 offset {days}
        );
    """
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return dict(row)


async def get_metrics_cexs_series_full():
    """
    Full cexs series.
    """
    qry = f"""
        select array_agg(mtr.timestamp / 1000 order by mtr.timestamp) as timestamps
            , array_agg(round(cgo.usd, 2) order by mtr.timestamp) as ergusd
            , array_agg(coinex  / 10^9 order by mtr.timestamp) as coinex
            , array_agg(gateio / 10^9  order by mtr.timestamp) as gateio
            , array_agg(kucoin / 10^9 order by mtr.timestamp) as kucoin
            , array_agg(total  / 10^9 order by mtr.timestamp) as total
            , array_agg(circulating_supply  / 10^9 order by mtr.timestamp) as circulating_supply
        from mtr.cexs_supply mtr
        left join cgo.price_at_first_of_day_block cgo
            on cgo.timestamp = mtr.timestamp
        ;
    """
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return dict(row)


async def get_metrics_cexs_list():
    """
    Known CEX addresses list.
    """
    qry = f"""
        select a.cex
            , a.address
            , coalesce(s.nano, 0) / 10^9 as balance
        from mtr.cex_addresses a
        left join mtr.cex_addresses_supply s
            on s.address = a.address
            and s.timestamp = (select timestamp from mtr.cex_addresses_supply order by 1 desc limit 1)
        order by 3 desc;
    """
    async with CONNECTION_POOL.acquire() as conn:
        rows = await conn.fetch(qry)
    return [dict(r) for r in rows]


async def get_metrics_age_series(days: int):
    """
    Last *days* days of supply age series.
    """
    qry = f"""
        select array_agg(mtr.timestamp / 1000 order by mtr.timestamp) as timestamps
            , array_agg(round(cgo.usd, 2) order by mtr.timestamp) as ergusd
            , array_agg(mean_age_days order by mtr.timestamp) as mean_age_days
        from con.mean_age_series_daily mtr
        left join cgo.price_at_first_of_day_block cgo
            on cgo.timestamp = mtr.timestamp
        where mtr.timestamp > (
            select timestamp
            from con.mean_age_series_daily
            order by 1 desc
            limit 1 offset {days}
        );
    """
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return dict(row)


async def get_metrics_age_series_full():
    """
    Full supply age series.
    """
    qry = f"""
        select array_agg(mtr.timestamp / 1000 order by mtr.timestamp) as timestamps
            , array_agg(round(cgo.usd, 2) order by mtr.timestamp) as ergusd
            , array_agg(mean_age_days order by mtr.timestamp) as mean_age_days
        from con.mean_age_series_daily mtr
        left join cgo.price_at_first_of_day_block cgo
            on cgo.timestamp = mtr.timestamp
        ;
    """
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return dict(row)


async def get_metrics_transfer_volume_series(days: int):
    """
    Last *days* days of transfer volume series.
    """
    qry = f"""
        select array_agg(mtr.timestamp / 1000 order by mtr.timestamp) as timestamps
            , array_agg(round(cgo.usd, 2) order by mtr.timestamp) as ergusd
            , array_agg(transferred_value / 10^9 order by mtr.timestamp) as transferred_volume
        from con.aggregate_series_daily mtr
        left join cgo.price_at_last_of_day_block cgo
            on cgo.timestamp = mtr.timestamp
        where mtr.timestamp > (
            select timestamp
            from con.aggregate_series_daily
            order by 1 desc
            limit 1 offset {days}
        );
    """
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return dict(row)


async def get_metrics_transfer_volume_series_full():
    """
    Full transfer volume series.
    """
    qry = f"""
        select array_agg(mtr.timestamp / 1000 order by mtr.timestamp) as timestamps
            , array_agg(round(cgo.usd, 2) order by mtr.timestamp) as ergusd
            , array_agg(transferred_value / 10^9 order by mtr.timestamp) as transferred_volume
        from con.aggregate_series_daily mtr
        left join cgo.price_at_last_of_day_block cgo
            on cgo.timestamp = mtr.timestamp
        ;
    """
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return dict(row)


async def get_metrics_transactions_series(days: int):
    """
    Last *days* days of transactions series.
    """
    qry = f"""
        select array_agg(mtr.timestamp / 1000 order by mtr.timestamp) as timestamps
            , array_agg(round(cgo.usd, 2) order by mtr.timestamp) as ergusd
            , array_agg(transactions order by mtr.timestamp) as transactions
        from con.aggregate_series_daily mtr
        left join cgo.price_at_last_of_day_block cgo
            on cgo.timestamp = mtr.timestamp
        where mtr.timestamp > (
            select timestamp
            from con.aggregate_series_daily
            order by 1 desc
            limit 1 offset {days}
        );
    """
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return dict(row)


async def get_metrics_transactions_series_full():
    """
    Full transactions series.
    """
    qry = f"""
        select array_agg(mtr.timestamp / 1000 order by mtr.timestamp) as timestamps
            , array_agg(round(cgo.usd, 2) order by mtr.timestamp) as ergusd
            , array_agg(transactions order by mtr.timestamp) as transactions
        from con.aggregate_series_daily mtr
        left join cgo.price_at_last_of_day_block cgo
            on cgo.timestamp = mtr.timestamp
        ;
    """
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return dict(row)


async def get_metrics_utxos_summary():
    """
    Latest summary of utxo series.
    """
    qry = f"""
        select col
            , latest as latest
            , diff_1d as diff_1d
            , diff_1w as diff_1w
            , diff_4w as diff_4w
            , diff_6m as diff_6m
            , diff_1y as diff_1y
        from mtr.unspent_boxes_change_summary;
    """
    async with CONNECTION_POOL.acquire() as conn:
        rows = await conn.fetch(qry)
    return [dict(r) for r in rows]


async def get_metrics_utxos_series(days: int):
    """
    Last *days* days of utxos series.
    """
    qry = f"""
        select array_agg(mtr.timestamp / 1000 order by mtr.timestamp) as timestamps
            , array_agg(round(cgo.usd, 2) order by mtr.timestamp) as ergusd
            , array_agg(boxes order by mtr.timestamp) as boxes
        from mtr.unspent_boxes mtr
        left join cgo.price_at_first_of_day_block cgo
            on cgo.timestamp = mtr.timestamp
        where mtr.timestamp > (
            select timestamp
            from mtr.unspent_boxes
            order by 1 desc
            limit 1 offset {days}
        );
    """
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return dict(row)


async def get_metrics_utxos_series_full():
    """
    Full utxos series.
    """
    qry = f"""
        select array_agg(mtr.timestamp / 1000 order by mtr.timestamp) as timestamps
            , array_agg(round(cgo.usd, 2) order by mtr.timestamp) as ergusd
            , array_agg(boxes order by mtr.timestamp) as boxes
        from mtr.unspent_boxes mtr
        left join cgo.price_at_first_of_day_block cgo
            on cgo.timestamp = mtr.timestamp
        ;
    """
    async with CONNECTION_POOL.acquire() as conn:
        row = await conn.fetchrow(qry)
    return dict(row)


async def get_metrics_utxos_list():
    """
    Dusty list.
    """
    qry = f"""
        select address, boxes
        from mtr.unspent_boxes_top_addresses
        order by 2 desc
        limit 100;
    """
    async with CONNECTION_POOL.acquire() as conn:
        rows = await conn.fetch(qry)
    return [dict(r) for r in rows]
