from typing import Dict
import pytest
import nest_asyncio
from mds.agg_mds.redis_cache import redis_cache


# https://github.com/encode/starlette/issues/440
nest_asyncio.apply()


@pytest.mark.asyncio
async def test_aggregate_commons(client):
    resp = client.get("/aggregate/commons")
    assert resp.status_code == 200
    assert resp.json() == None

    await redis_cache.update_metadata(
        "commons1",
        [],
        {},
        [],
        None,
        None,
    )
    await redis_cache.update_metadata(
        "commons2",
        [],
        {},
        [],
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

    await redis_cache.update_metadata(
        "commons1",
        [
            {
                "study1": {},
            }
        ],
        {},
        ["study1"],
        None,
        None,
    )
    await redis_cache.update_metadata(
        "commons2",
        [
            {
                "study2": {},
            }
        ],
        {},
        ["study2"],
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
async def test_aggregate_metdata_name(client):
    resp = client.get("/aggregate/metadata/commons1")
    assert resp.status_code == 404
    assert resp.json() == {"detail": "Commons not found: commons1"}

    await redis_cache.update_metadata(
        "commons1",
        [
            {
                "study1": {},
            }
        ],
        {},
        ["study1"],
        None,
        None,
    )
    resp = client.get("/aggregate/metadata/commons1")
    assert resp.status_code == 200
    assert resp.json() == [{"study1": {}}]


@pytest.mark.asyncio
async def test_aggregate_metdata_tags(client):
    resp = client.get("/aggregate/metadata/commons1/tags")
    assert resp.status_code == 404
    assert resp.json() == {"detail": "Commons not found: commons1"}

    await redis_cache.update_metadata(
        "commons1",
        [
            {
                "study1": {},
            }
        ],
        {},
        ["study1"],
        ["mytag1"],
        None,
    )
    resp = client.get("/aggregate/metadata/commons1/tags")
    assert resp.status_code == 200
    assert resp.json() == ["mytag1"]


@pytest.mark.asyncio
async def test_aggregate_metdata_info(client):
    resp = client.get("/aggregate/metadata/commons1/info")
    assert resp.status_code == 404
    assert resp.json() == {"detail": "Commons not found: commons1"}

    await redis_cache.update_metadata(
        "commons1",
        [
            {
                "study1": {},
            }
        ],
        {},
        ["guid1"],
        None,
        {"commons_url": "http://commons"},
    )
    resp = client.get("/aggregate/metadata/commons1/info")
    assert resp.status_code == 200
    assert resp.json() == {"commons_url": "http://commons"}


@pytest.mark.asyncio
async def test_aggregate_metdata_field_to_columns(client):
    resp = client.get("/aggregate/metadata/commons1/field_to_columns")
    assert resp.status_code == 404
    assert resp.json() == {"detail": "Not found: commons1"}

    await redis_cache.update_metadata(
        "commons1",
        [
            {
                "study1": {},
            }
        ],
        {"fields": {"some_key": "other_key"}},
        ["study1"],
        None,
        {"commons_url": "http://commons"},
    )
    resp = client.get("/aggregate/metadata/commons1/field_to_columns")
    assert resp.status_code == 200
    assert resp.json() == {"fields": {"some_key": "other_key"}}


@pytest.mark.asyncio
async def test_aggregate_metdata_name_guid(client):
    resp = client.get("/aggregate/metadata/commons1/guid/study2:path")
    assert resp.status_code == 404
    assert resp.json() == {"detail": "Not found: commons1/study2"}

    await redis_cache.update_metadata(
        "commons1",
        [
            {
                "study1": {},
            },
            {
                "study2": {},
            },
        ],
        {"fields": {"some_key": "other_key"}},
        ["study1", "study2"],
        None,
        {"commons_url": "http://commons"},
    )
    resp = client.get("/aggregate/metadata/commons1/guid/study2:path")
    assert resp.status_code == 200
    assert resp.json() == {"study2": {}}
