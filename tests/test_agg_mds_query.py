from typing import Dict
import pytest
import nest_asyncio
from mds.agg_mds import datastore


# https://github.com/encode/starlette/issues/440
nest_asyncio.apply()


@pytest.mark.asyncio
async def test_aggregate_commons(client):
    resp = client.get("/aggregate/commons")
    assert resp.status_code == 200
    assert resp.json() == None

    await datastore.update_metadata(
        "commons1",
        [],
        [],
        None,
        None,
        None,
    )
    await datastore.update_metadata(
        "commons2",
        [],
        [],
        None,
        None,
        None,
    )
    resp = client.get("/aggregate/commons")
    assert resp.status_code == 200
    assert resp.json() == {"commons": ["commons1", "commons2"]}


@pytest.mark.asyncio
async def test_aggregate_metadata(client):
    resp = client.get("/aggregate/metadata")
    assert resp.status_code == 200
    assert resp.json() == {}

    await datastore.update_metadata(
        "commons1",
        [
            {
                "study1": {},
            }
        ],
        ["study1"],
        None,
        None,
        None,
    )
    await datastore.update_metadata(
        "commons2",
        [
            {
                "study2": {},
            }
        ],
        ["study2"],
        None,
        None,
        None,
    )
    resp = client.get("/aggregate/metadata")
    assert resp.status_code == 200
    assert resp.json() == {
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


@pytest.mark.asyncio
async def test_aggregate_metadata_name(client):
    resp = client.get("/aggregate/metadata/commons1")
    assert resp.status_code == 404
    assert resp.json() == {
        "detail": {
            "code": 404,
            "message": "no common exists with the given: commons1",
        }
    }

    await datastore.update_metadata(
        "commons1",
        [
            {
                "study1": {},
            }
        ],
        ["study1"],
        None,
        None,
        None,
    )
    resp = client.get("/aggregate/metadata/commons1")
    assert resp.status_code == 200
    assert resp.json() == [{"study1": {}}]


@pytest.mark.asyncio
async def test_aggregate_metadata_tags(client):
    resp = client.get("/aggregate/metadata/commons1/tags")
    assert resp.status_code == 404
    assert resp.json() == {
        "detail": {
            "code": 404,
            "message": "no common exists with the given: commons1",
        }
    }

    await datastore.update_metadata(
        "commons1",
        [
            {
                "study1": {},
            }
        ],
        ["study1"],
        ["mytag1"],
        None,
        None,
    )
    resp = client.get("/aggregate/metadata/commons1/tags")
    assert resp.status_code == 200
    assert resp.json() == ["mytag1"]


@pytest.mark.asyncio
async def test_aggregate_metadata_info(client):
    resp = client.get("/aggregate/metadata/commons1/info")
    assert resp.status_code == 404
    assert resp.json() == {
        "detail": {
            "code": 404,
            "message": "no common exists with the given: commons1",
        }
    }

    await datastore.update_metadata(
        "commons1",
        [
            {
                "study1": {},
            }
        ],
        ["guid1"],
        None,
        {"commons_url": "http://commons"},
        None,
    )
    resp = client.get("/aggregate/metadata/commons1/info")
    assert resp.status_code == 200
    assert resp.json() == {"commons_url": "http://commons"}


@pytest.mark.asyncio
async def test_metadata_aggregations(client):
    resp = client.get("/aggregate/metadata/commons1/aggregations")
    assert resp.status_code == 404
    assert resp.json() == {
        "detail": {
            "code": 404,
            "message": "no common exists with the given: commons1",
        }
    }


@pytest.mark.asyncio
async def test_aggregate_metadata_name_guid(client):
    resp = client.get("/aggregate/metadata/commons1/guid/study2:path")
    assert resp.status_code == 404
    assert resp.json() == {
        "detail": {
            "code": 404,
            "message": "no common/guid exists with the given: commons1/study2",
        }
    }

    await datastore.update_metadata(
        "commons1",
        [
            {
                "study1": {},
            },
            {
                "study2": {},
            },
        ],
        ["study1", "study2"],
        None,
        {"commons_url": "http://commons"},
        None,
    )
    resp = client.get("/aggregate/metadata/commons1/guid/study2:path")
    assert resp.status_code == 200
    assert resp.json() == {"study2": {}}
