import os
import pytest


from ..main import db


@pytest.mark.asyncio
async def test_get_latest_block_height():
    height = await db.get_latest_block_height()
    assert height > 500000


@pytest.mark.asyncio
async def test_get_oracle_pools_commits_for_ergusd_pool():
    pool_id = 1
    data = await db.get_oracle_pool_commits(pool_id)
    assert len(data) == 11
    assert data['9eh9WDsRAsujyFx4x7YeSoxrLCqmhuQihDwgsWVqEuXte7QJRCU'] > 100