from typing import List
from typing import Optional
from fastapi import APIRouter
from fastapi import HTTPException
from fastapi import Query
from fastapi import Request
from pydantic import BaseModel

router = r = APIRouter()


class MetricsOverview(BaseModel):
    counts_p2pks: int
    counts_contracts: int
    counts_miners: int
    dist_1prc_p2pks: float
    dist_1prc_contracts: float
    dist_1prc_miners: float
    supply_prc_on_p2pks: float
    supply_on_cexs: float
    # supply_age_overall: float
    usage_24h_volume: int
    usage_24h_transactions: int
    usage_utxos: int


@r.get(
    "/",
    response_model=MetricsOverview,
    summary="Overview of latest metrics",
)
async def metrics_overview(request: Request):
    query = f"""
        with circ_supply as (
            select current as total
            from mtr.supply_composition_summary
            where label = 'total'
        )
        select '00 p2pk counts' as name, (select total from mtr.address_counts_by_balance_p2pk order by height desc limit 1) as value
        union
        select '01 contract counts' as name, (select total from mtr.address_counts_by_balance_contracts order by height desc limit 1) as value
        union
        select '02 mining contract counts' as name, (select total from mtr.address_counts_by_balance_miners order by height desc limit 1) as value
        union
        select '03 dist p2pks' as name, (
            select m.top_1_prc::numeric / s.total
            from mtr.supply_on_top_addresses_p2pk m, circ_supply s
            order by m.height desc limit 1
        ) as value
        union
        select '04 dist contracts' as name, (
            select m.top_1_prc::numeric / s.total
            from mtr.supply_on_top_addresses_contracts m, circ_supply s
            order by m.height desc limit 1
        ) as value
        union
        select '05 dist mining contracts' as name, (
            select m.top_1_prc::numeric / s.total
            from mtr.supply_on_top_addresses_miners m, circ_supply s
            order by m.height desc limit 1
        ) as value
        union
        select '06 supply on p2pks' as name, (
            select p2pks::numeric / (p2pks + cex_main + cex_deposits + contracts + miners + treasury)
            from mtr.supply_composition
            order by height desc limit 1
        ) as value
        union
        select '07 supply on exchanges' as name, (select total from mtr.cex_supply order by height desc limit 1) as value
        union
        select '08 supply age' as name, (select overall from mtr.supply_age_days order by height desc limit 1) as value
        union
        select '09 volume' as name, (select daily_1d from mtr.volume order by height desc limit 1) as value
        union
        select '10 transactions' as name, (select daily_1d from mtr.transactions order by height desc limit 1) as value
        union
        select '11 utxos' as name, (select value from mtr.utxos order by height desc limit 1) as value
        order by 1;
    """
    async with request.app.state.db.acquire() as conn:
        rows = await conn.fetch(query)
    return {
        "counts_p2pks": rows[0]["value"],
        "counts_contracts": rows[1]["value"],
        "counts_miners": rows[2]["value"],
        "dist_1prc_p2pks": rows[3]["value"],
        "dist_1prc_contracts": rows[4]["value"],
        "dist_1prc_miners": rows[5]["value"],
        "supply_prc_on_p2pks": rows[6]["value"],
        "supply_on_cexs": rows[7]["value"],
        # "supply_age_overall": rows[8]["value"],
        "usage_24h_volume": rows[9]["value"],
        "usage_24h_transactions": rows[10]["value"],
        "usage_utxos": rows[11]["value"],
    }
