import pytest
import nest_asyncio
from unittest.mock import patch
from conftest import AsyncMock
from mds.agg_mds import datastore

# https://github.com/encode/starlette/issues/440
nest_asyncio.apply()


@pytest.mark.asyncio
async def test_init():
    with patch("mds.agg_mds.datastore.client", AsyncMock()) as mock_client:
        await datastore.init("host", 9999)
    mock_client.init.assert_called_with("host", 9999)


@pytest.mark.asyncio
async def test_drop_all():
    with patch("mds.agg_mds.datastore.client", AsyncMock()) as mock_client:
        await datastore.drop_all()
    mock_client.drop_all.assert_called_with()


@pytest.mark.asyncio
async def test_drop_all_temp_indexes():
    with patch("mds.agg_mds.datastore.client", AsyncMock()) as mock_client:
        await datastore.drop_all_temp_indexes()
    mock_client.drop_all_temp_indexes.assert_called_with()


@pytest.mark.asyncio
async def test_create_indexes():
    with patch("mds.agg_mds.datastore.client", AsyncMock()) as mock_client:
        await datastore.create_indexes("{}")
    mock_client.create_indexes.assert_called_with("{}")


@pytest.mark.asyncio
async def test_create_temp_indexes():
    with patch("mds.agg_mds.datastore.client", AsyncMock()) as mock_client:
        await datastore.create_temp_indexes("{}")
    mock_client.create_temp_indexes.assert_called_with("{}")


@pytest.mark.asyncio
async def test_clone_temp_indexes_to_real_indexes():
    with patch("mds.agg_mds.datastore.client", AsyncMock()) as mock_client:
        await datastore.clone_temp_indexes_to_real_indexes()
    mock_client.clone_temp_indexes_to_real_indexes.assert_called_with()


@pytest.mark.asyncio
async def test_close():
    with patch("mds.agg_mds.datastore.client", AsyncMock()) as mock_client:
        await datastore.close()
    mock_client.close.assert_called_with()


@pytest.mark.asyncio
async def test_get_status():
    with patch("mds.agg_mds.datastore.client", AsyncMock()) as mock_client:
        await datastore.get_status()
    mock_client.get_status.assert_called_with()


@pytest.mark.asyncio
async def test_update_metadata():
    with patch("mds.agg_mds.datastore.client", AsyncMock()) as mock_client:
        await datastore.update_metadata()
    mock_client.update_metadata.assert_called_with()


@pytest.mark.asyncio
async def test_update_metadata_to_temp_index():
    with patch("mds.agg_mds.datastore.client", AsyncMock()) as mock_client:
        await datastore.update_metadata_to_temp_index()
    mock_client.update_metadata_to_temp_index.assert_called_with()


@pytest.mark.asyncio
async def test_update_global_info():
    with patch("mds.agg_mds.datastore.client", AsyncMock()) as mock_client:
        await datastore.update_global_info()
    mock_client.update_global_info.assert_called_with()


@pytest.mark.asyncio
async def test_update_global_info_to_temp_index():
    with patch("mds.agg_mds.datastore.client", AsyncMock()) as mock_client:
        await datastore.update_global_info_to_temp_index()
    mock_client.update_global_info_to_temp_index.assert_called_with()


@pytest.mark.asyncio
async def test_update_config_info():
    with patch("mds.agg_mds.datastore.client", AsyncMock()) as mock_client:
        await datastore.update_config_info()
    mock_client.update_config_info.assert_called_with()


@pytest.mark.asyncio
async def test_update_config_info_to_temp_index():
    with patch("mds.agg_mds.datastore.client", AsyncMock()) as mock_client:
        await datastore.update_config_info_to_temp_index()
    mock_client.update_config_info_to_temp_index.assert_called_with()


@pytest.mark.asyncio
async def test_get_commons_metadata():
    with patch("mds.agg_mds.datastore.client", AsyncMock()) as mock_client:
        await datastore.get_commons_metadata()
    mock_client.get_commons_metadata.assert_called_with()


@pytest.mark.asyncio
async def test_get_all_named_commons_metadata():
    with patch("mds.agg_mds.datastore.client", AsyncMock()) as mock_client:
        await datastore.get_all_named_commons_metadata()
    mock_client.get_all_named_commons_metadata.assert_called_with()


@pytest.mark.asyncio
async def test_get_all_tags():
    with patch("mds.agg_mds.datastore.client", AsyncMock()) as mock_client:
        await datastore.get_all_tags()
    mock_client.metadata_tags.assert_called_with()


@pytest.mark.asyncio
async def test_get_by_guid():
    with patch("mds.agg_mds.datastore.client", AsyncMock()) as mock_client:
        await datastore.get_by_guid("123")
    mock_client.get_by_guid.assert_called_with("123")


@pytest.mark.asyncio
async def test_get_commons_attribute():
    with patch("mds.agg_mds.datastore.client", AsyncMock()) as mock_client:
        await datastore.get_commons_attribute()
    mock_client.get_commons_attribute.assert_called_with()


@pytest.mark.asyncio
async def test_get_commons():
    with patch("mds.agg_mds.datastore.client", AsyncMock()) as mock_client:
        await datastore.get_commons()
    mock_client.get_commons.assert_called_with()


@pytest.mark.asyncio
async def test_get_all_metadata():
    with patch("mds.agg_mds.datastore.client", AsyncMock()) as mock_client:
        await datastore.get_all_metadata()
    mock_client.get_all_metadata.assert_called_with()
