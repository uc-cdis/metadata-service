import pytest
import nest_asyncio
from unittest.mock import patch
from conftest import AsyncMock
from mds.agg_mds import datastore

# https://github.com/encode/starlette/issues/440
nest_asyncio.apply()


@pytest.mark.asyncio
async def test_init():
    with patch("mds.agg_mds.datastore.redis_client", AsyncMock()) as mock_redis_cache:
        await datastore.init("host", 9999)
    mock_redis_cache.init_cache.assert_called_with("host", 9999)


@pytest.mark.asyncio
async def test_drop_all():
    with patch("mds.agg_mds.datastore.redis_client", AsyncMock()) as mock_redis_cache:
        await datastore.drop_all()
    mock_redis_cache.json_sets.assert_called_with("commons", [])


@pytest.mark.asyncio
async def test_close():
    with patch("mds.agg_mds.datastore.redis_client", AsyncMock()) as mock_redis_cache:
        await datastore.close()
    mock_redis_cache.close.assert_called_with()


@pytest.mark.asyncio
async def test_get_status():
    with patch("mds.agg_mds.datastore.redis_client", AsyncMock()) as mock_redis_cache:
        await datastore.get_status()
    mock_redis_cache.get_status.assert_called_with()


@pytest.mark.asyncio
async def test_update_metadata():
    with patch("mds.agg_mds.datastore.redis_client", AsyncMock()) as mock_redis_cache:
        await datastore.update_metadata()
    mock_redis_cache.update_metadata.assert_called_with()


@pytest.mark.asyncio
async def test_get_commons_metadata():
    with patch("mds.agg_mds.datastore.redis_client", AsyncMock()) as mock_redis_cache:
        await datastore.get_commons_metadata()
    mock_redis_cache.get_commons_metadata.assert_called_with()


@pytest.mark.asyncio
async def test_get_all_named_commons_metadata():
    with patch("mds.agg_mds.datastore.redis_client", AsyncMock()) as mock_redis_cache:
        await datastore.get_all_named_commons_metadata()
    mock_redis_cache.get_all_named_commons_metadata.assert_called_with()


@pytest.mark.asyncio
async def test_get_commons_metadata_guid():
    with patch("mds.agg_mds.datastore.redis_client", AsyncMock()) as mock_redis_cache:
        await datastore.get_commons_metadata_guid()
    mock_redis_cache.get_commons_metadata_guid.assert_called_with()


@pytest.mark.asyncio
async def test_get_commons_attribute():
    with patch("mds.agg_mds.datastore.redis_client", AsyncMock()) as mock_redis_cache:
        await datastore.get_commons_attribute()
    mock_redis_cache.get_commons_attribute.assert_called_with()


@pytest.mark.asyncio
async def test_get_commons():
    with patch("mds.agg_mds.datastore.redis_client", AsyncMock()) as mock_redis_cache:
        await datastore.get_commons()
    mock_redis_cache.get_commons.assert_called_with()


@pytest.mark.asyncio
async def test_get_all_metadata():
    with patch("mds.agg_mds.datastore.redis_client", AsyncMock()) as mock_redis_cache:
        await datastore.get_all_metadata()
    mock_redis_cache.get_all_metadata.assert_called_with()
