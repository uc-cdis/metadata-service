import json
from unittest.mock import patch
from conftest import AsyncMock
import pytest
from mds.agg_mds.redis_cache import RedisCache
import mds.agg_mds.redis_cache
import nest_asyncio
import fakeredis.aioredis
import aioredis


@pytest.mark.asyncio
async def test_init_cache():
    cache = RedisCache()

    async def mock_pool(address):
        return f"mock:result:{address}"

    with patch.object(mds.agg_mds.redis_cache, "create_redis_pool", mock_pool):
        await cache.init_cache()
        assert cache.redis_cache == "mock:result:redis://0.0.0.0:6379/0?encoding=utf-8"


@pytest.mark.asyncio
async def test_keys():
    cache = RedisCache()
    cache.redis_cache = await fakeredis.aioredis.create_redis_pool()
    await cache.redis_cache.set("commons1", "some data")
    keys = await cache.keys("commons1")
    assert keys == [b"commons1"]


@pytest.mark.asyncio
async def test_json_get():
    cache = RedisCache()
    cache.redis_cache = await fakeredis.aioredis.create_redis_pool()

    with patch.object(cache.redis_cache, "execute", AsyncMock(return_value=None)):
        resp = await cache.json_get("some_key")
        assert resp == None

    with patch.object(
        cache.redis_cache, "execute", AsyncMock(return_value=json.dumps({}))
    ):
        resp = await cache.json_get("some_key")
        assert resp == {}


@pytest.mark.asyncio
async def test_get_commons_metadata():
    cache = RedisCache()
    cache.redis_cache = await fakeredis.aioredis.create_redis_pool()

    with patch.object(cache, "json_get", AsyncMock(return_value=None)):
        keys = await cache.get_commons_metadata("commons1", 3, 2)
        assert keys == None

    with patch.object(
        cache, "json_get", AsyncMock(return_value=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    ):
        keys = await cache.get_commons_metadata("commons1", 3, 2)
        assert keys == [3, 4, 5]


@pytest.mark.asyncio
async def test_get_commons_metadata_guid():
    cache = RedisCache()
    cache.redis_cache = await fakeredis.aioredis.create_redis_pool()

    patch.object(cache, "json_get", AsyncMock(return_value=[])).start()
    patch.object(cache, "json_arr_index", AsyncMock(return_value=None)).start()

    keys = await cache.get_commons_metadata_guid("commons1", "guid1")
    assert keys == None
