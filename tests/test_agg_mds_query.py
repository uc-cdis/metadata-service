from typing import Dict
import pytest
import nest_asyncio
from mds.agg_mds import datastore
from unittest.mock import patch
from conftest import AsyncMock


# https://github.com/encode/starlette/issues/440
nest_asyncio.apply()


@pytest.mark.asyncio
async def test_aggregate_commons(client):
    with patch.object(
        datastore, "get_commons", AsyncMock(return_value={})
    ) as datastore_mock:
        resp = client.get("/aggregate/commons")
        assert resp.status_code == 200
        assert resp.json() == {}
        datastore.get_commons.assert_called_with()

    with patch.object(
        datastore,
        "get_commons",
        AsyncMock(return_value={"commons": ["commons1", "commons2"]}),
    ) as datastore_mock:
        resp = client.get("/aggregate/commons")
        assert resp.status_code == 200
        assert resp.json() == {"commons": ["commons1", "commons2"]}
        datastore.get_commons.assert_called_with()


@pytest.mark.asyncio
async def test_aggregate_metadata(client):
    with patch.object(
        datastore, "get_all_metadata", AsyncMock(return_value=[])
    ) as datastore_mock:
        resp = client.get("/aggregate/metadata")
        assert resp.status_code == 200
        assert resp.json() == []
        datastore.get_all_metadata.assert_called_with(20, 0)

    mock_data = {
        "commons1": [
            {
                "study1": {},
            }
        ],
        "commons2": [
            {
                "study2": {},
            }
        ],
    }

    with patch.object(
        datastore, "get_all_metadata", AsyncMock(return_value=mock_data)
    ) as datastore_mock:
        resp = client.get("/aggregate/metadata")
        assert resp.status_code == 200
        assert resp.json() == mock_data
        datastore.get_all_metadata.assert_called_with(20, 0)


@pytest.mark.asyncio
async def test_aggregate_metadata_name(client):
    with patch.object(
        datastore, "get_all_named_commons_metadata", AsyncMock(return_value=None)
    ) as datastore_mock:
        resp = client.get("/aggregate/metadata/commons1")
        assert resp.status_code == 404
        assert resp.json() == {
            "detail": {
                "code": 404,
                "message": "no common exists with the given: commons1",
            }
        }
        datastore.get_all_named_commons_metadata.assert_called_with("commons1")

    with patch.object(
        datastore,
        "get_all_named_commons_metadata",
        AsyncMock(return_value=[{"study1": {}}]),
    ) as datastore_mock:
        resp = client.get("/aggregate/metadata/commons1")
        assert resp.status_code == 200
        assert resp.json() == [{"study1": {}}]
        datastore.get_all_named_commons_metadata.assert_called_with("commons1")


@pytest.mark.asyncio
async def test_aggregate_metadata_tags(client):
    with patch.object(
        datastore, "get_commons_attribute", AsyncMock(return_value=None)
    ) as datastore_mock:
        resp = client.get("/aggregate/metadata/commons1/tags")
        assert resp.status_code == 404
        assert resp.json() == {
            "detail": {
                "code": 404,
                "message": "no common exists with the given: commons1",
            }
        }

    with patch.object(
        datastore, "get_commons_attribute", AsyncMock(return_value=["mytag1"])
    ) as datastore_mock:
        resp = client.get("/aggregate/metadata/commons1/tags")
        assert resp.status_code == 200
        assert resp.json() == ["mytag1"]
        datastore.get_commons_attribute.assert_called_with("commons1", "tags")


@pytest.mark.asyncio
async def test_aggregate_metadata_info(client):
    with patch.object(
        datastore, "get_commons_attribute", AsyncMock(return_value=None)
    ) as datastore_mock:
        resp = client.get("/aggregate/metadata/commons1/info")
        assert resp.status_code == 404
        assert resp.json() == {
            "detail": {
                "code": 404,
                "message": "no common exists with the given: commons1",
            }
        }
        datastore.get_commons_attribute.assert_called_with("commons1", "info")

    with patch.object(
        datastore,
        "get_commons_attribute",
        AsyncMock(return_value={"commons_url": "http://commons"}),
    ) as datastore_mock:
        resp = client.get("/aggregate/metadata/commons1/info")
        assert resp.status_code == 200
        assert resp.json() == {"commons_url": "http://commons"}
        datastore.get_commons_attribute.assert_called_with("commons1", "info")


@pytest.mark.asyncio
async def test_metadata_aggregations(client):
    with patch.object(
        datastore, "get_aggregations", AsyncMock(return_value=None)
    ) as datastore_mock:
        resp = client.get("/aggregate/metadata/commons1/aggregations")
        assert resp.status_code == 404
        assert resp.json() == {
            "detail": {
                "code": 404,
                "message": "no common exists with the given: commons1",
            }
        }
        datastore.get_aggregations.assert_called_with("commons1")


@pytest.mark.asyncio
async def test_aggregate_metadata_name_guid(client):
    with patch.object(
        datastore, "get_by_guid", AsyncMock(return_value=None)
    ) as datastore_mock:
        resp = client.get("/aggregate/metadata/guid/123")
        assert resp.status_code == 404
        assert resp.json() == {
            "detail": {
                "code": 404,
                "message": "no entry exists with the given guid: 123",
            }
        }
        datastore.get_by_guid.assert_called_with("123")

    with patch.object(
        datastore, "get_by_guid", AsyncMock(return_value={"study2": {}})
    ) as datastore_mock:
        resp = client.get("/aggregate/metadata/guid/123")
        assert resp.status_code == 200
        assert resp.json() == {"study2": {}}
        datastore.get_by_guid.assert_called_with("123")
