import json
from unittest.mock import patch, call, MagicMock
from conftest import AsyncMock
import pytest
import mds
from mds.agg_mds.datastore.redis_cache import RedisCache
import mds.agg_mds.datastore.redis_cache
import nest_asyncio
import fakeredis.aioredis
import aioredis
from datetime import datetime


@pytest.mark.asyncio
async def test_init_cache():
    cache = RedisCache()

    async def mock_pool(address):
        return f"mock:result:{address}"

    with patch.object(
        mds.agg_mds.datastore.redis_cache, "create_redis_pool", mock_pool
    ):
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
async def test_get_status():
    cache = RedisCache()

    mock_data = ["status2", "status1", ["commons1", "commons2"]]

    async def mock_json_get(arg1, arg2=None):
        return mock_data.pop()

    patch.object(cache, "json_get", mock_json_get).start()

    assert await cache.get_status() == {
        "commons1": "status1",
        "commons2": "status2",
    }


@pytest.mark.asyncio
async def test_close():
    cache = RedisCache()
    cache.redis_cache = await fakeredis.aioredis.create_redis_pool()

    patch.object(cache.redis_cache, "close", MagicMock()).start()
    patch.object(cache.redis_cache, "wait_closed", AsyncMock()).start()

    await cache.close()

    cache.redis_cache.close.assert_called_with()
    cache.redis_cache.wait_closed.assert_called_with()


@pytest.mark.asyncio
async def test_update_metadata():
    cache = RedisCache()
    cache.json_sets = AsyncMock()
    cache.json_arr_appends = AsyncMock()

    now = datetime.now()
    with patch("mds.agg_mds.datastore.redis_cache.datetime") as mock_date:
        mock_date.now = MagicMock(return_value=now)
        await cache.update_metadata("commons1", [], [], {}, {}, {})

    cache.json_sets.assert_has_calls(
        [
            call("commons1", {}),
            call("commons1", [], ".metadata"),
            call("commons1", [], ".guids"),
            call("commons1", {}, ".tags"),
            call("commons1", {}, ".info"),
            call("commons1", {}, ".aggregations"),
            call(
                "commons1.status",
                {
                    "last_update": now.strftime("%Y%m%d%H%M%S"),
                    "error": 0,
                    "count": "none",
                },
            ),
        ]
    )
    cache.json_arr_appends.assert_called_with("commons", "commons1")


@pytest.mark.asyncio
async def test_get_commons_metadata():
    cache = RedisCache()

    with patch.object(cache, "json_get", AsyncMock(return_value=None)):
        assert await cache.get_commons_metadata("commons1", 3, 2) == None

    with patch.object(
        cache, "json_get", AsyncMock(return_value=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
    ):
        assert await cache.get_commons_metadata("commons1", 3, 2) == [3, 4, 5]


@pytest.mark.asyncio
async def test_get_commons_metadata_guid():
    cache = RedisCache()

    patch.object(cache, "json_get", AsyncMock(return_value=[])).start()
    patch.object(cache, "json_arr_index", AsyncMock(return_value=None)).start()

    assert await cache.get_commons_metadata_guid("commons1", "guid1") == None

    patch.object(
        cache, "json_get", AsyncMock(return_value=["commons0", "commons1", "commons2"])
    ).start()
    patch.object(cache, "json_arr_index", AsyncMock(return_value=1)).start()

    assert await cache.get_commons_metadata_guid("commons1", "guid1") == "commons1"


@pytest.mark.asyncio
async def test_get_commons_attribute():
    cache = RedisCache()
    cache.json_get = AsyncMock()

    await cache.get_commons_attribute("something", "other")

    cache.json_get.assert_called_with("something", "other")


@pytest.mark.asyncio
async def test_get_all_metadata():
    cache = RedisCache()

    patch.object(cache, "json_get", AsyncMock(return_value=None)).start()

    assert await cache.get_all_metadata(2, 4) == {}

    mock_data = [
        [None, "recordX2", "recordY2", "recordZ2", None],
        [None, None, None, "recordX1", "recordY1", "recordZ1", None],
        ["commons1", "commons2"],
    ]

    async def mock_json_get(arg1, arg2=None):
        return mock_data.pop()

    patch.object(cache, "json_get", mock_json_get).start()

    assert await cache.get_all_metadata(2, 4) == {
        "commons1": ["recordY1", "recordZ1"],
        "commons2": [None],
    }
