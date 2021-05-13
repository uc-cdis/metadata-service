import json
from unittest.mock import patch
from conftest import AsyncMock
import pytest
from mds.agg_mds.redis_cache import RedisCache
import nest_asyncio
import fakeredis.aioredis


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
async def test_get_commons_metadata_guid():
    cache = RedisCache()
    cache.redis_cache = await fakeredis.aioredis.create_redis_pool()

    async def mock_json_get(key, path):
        return []

    async def mock_json_arr_index(key, guid):
        return None

    patch.object(cache, "json_get", mock_json_get).start()
    patch.object(cache, "json_arr_index", mock_json_arr_index).start()

    keys = await cache.get_commons_metadata_guid("commons1", "guid1")
    assert keys == None
