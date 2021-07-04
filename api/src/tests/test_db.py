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


@pytest.mark.asyncio
async def test_get_oracle_pools_commit_stats_for_ergusd_pooo():
    data = await db.get_oracle_pool_commit_stats_ergusd()
    assert len(data) == 11
    assert 'address' in data[0]
    assert data[0]['commits'] > 100
    assert data[0]['accepted_commits'] > 100
    assert 'first_commit' in data[0]
    assert 'last_commit' in data[0]