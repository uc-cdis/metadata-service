import json
from unittest.mock import patch, call, MagicMock
from conftest import AsyncMock
import pytest
import mds
from mds.agg_mds.datastore import elasticsearch_dao
from mds.agg_mds.datastore.elasticsearch_dao import MAPPING
import nest_asyncio


@pytest.mark.asyncio
async def test_init():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.Elasticsearch", MagicMock()
    ) as mock_client:
        await elasticsearch_dao.init("myhost")
    mock_client.assert_called_with(["myhost"], port=9200, scheme="http")


@pytest.mark.asyncio
async def test_drop_all():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.indices",
        MagicMock(),
    ) as mock_indices:
        await elasticsearch_dao.drop_all()
    mock_indices.delete.assert_called_with(index="_all", ignore=[400, 404])
    mock_indices.create.assert_has_calls(
        [
            call(body=MAPPING, index="commons-index"),
            call(index="commons-info-index"),
        ],
        any_order=True,
    )


@pytest.mark.asyncio
async def test_update_metadata():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.index",
        MagicMock(),
    ) as mock_index:
        await elasticsearch_dao.update_metadata(
            "my_commons",
            [
                {
                    "my_id": {
                        "gen3_discovery": {
                            "one": "one",
                        }
                    }
                }
            ],
            [],
            {},
            {},
        )
    mock_index.assert_has_calls(
        [
            call(
                body={},
                doc_type="commons-info",
                id="my_commons",
                index="commons-info-index",
            ),
            call(
                body={"one": "one"},
                doc_type="commons",
                id="my_id",
                index="commons-index",
            ),
        ],
    )


@pytest.mark.asyncio
async def test_get_status():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client",
        MagicMock(),
    ) as mock_client:
        await elasticsearch_dao.get_status()

    mock_client.ping.assert_called_with()


@pytest.mark.asyncio
async def test_get_commons():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.search",
        MagicMock(),
    ) as mock_search:
        await elasticsearch_dao.get_commons()
        mock_search.assert_called_with(
            index="commons-index",
            body={
                "size": 0,
                "aggs": {"commons_names": {"terms": {"field": "commons_name.keyword"}}},
            },
        )

    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.search",
        MagicMock(side_effect=Exception("some error")),
    ) as mock_search:
        assert await elasticsearch_dao.get_commons() == []


@pytest.mark.asyncio
async def test_get_all_metadata():
    response = {
        "hits": {"hits": [{"_id": 1, "_source": {"commons_name": "my-commons"}}]}
    }

    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.search",
        MagicMock(return_value=response),
    ) as mock_search:
        await elasticsearch_dao.get_all_metadata(5, 9)
        mock_search.assert_called_with(
            index="commons-index",
            body={"size": 5, "from": 9, "query": {"match_all": {}}},
        )

    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.search",
        MagicMock(side_effect=Exception("some error")),
    ) as mock_search:
        assert await elasticsearch_dao.get_all_metadata(5, 9) == {}


@pytest.mark.asyncio
async def test_get_all_named_commons_metadata():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client", MagicMock()
    ) as mock_client:
        await elasticsearch_dao.get_all_named_commons_metadata("my-commons")
        mock_client.search.assert_called_with(
            index="commons-index",
            body={"query": {"match": {"commons_name.keyword": "my-commons"}}},
        )

    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.search",
        MagicMock(side_effect=Exception("some error")),
    ) as mock_search:
        assert (
            await elasticsearch_dao.get_all_named_commons_metadata("my-commons") == {}
        )


@pytest.mark.asyncio
async def test_metadata_tags():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client", MagicMock()
    ) as mock_client:
        await elasticsearch_dao.metadata_tags("my-commons")
        mock_client.search.assert_called_with(
            index="commons-index",
            body={
                "size": 0,
                "aggs": {
                    "tags": {
                        "nested": {"path": "tags"},
                        "aggs": {
                            "categories": {
                                "terms": {"field": "tags.category.keyword"},
                                "aggs": {
                                    "name": {"terms": {"field": "tags.name.keyword"}}
                                },
                            }
                        },
                    }
                },
            },
        )

    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.search",
        MagicMock(side_effect=Exception("some error")),
    ) as mock_search:
        assert await elasticsearch_dao.metadata_tags("my-commons") == []


@pytest.mark.asyncio
async def test_get_commons_attribute():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client", MagicMock()
    ) as mock_client:
        await elasticsearch_dao.get_commons_attribute("my-commons", "attribute")
        mock_client.search.assert_called_with(
            index="commons-info-index",
            body={"query": {"terms": {"_id": ["my-commons"]}}},
        )

    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.search",
        MagicMock(side_effect=Exception("some error")),
    ) as mock_search:
        assert (
            await elasticsearch_dao.get_commons_attribute("my-commons", "attribute")
            == None
        )


@pytest.mark.asyncio
async def test_get_aggregations():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client", MagicMock()
    ) as mock_client:
        await elasticsearch_dao.get_aggregations("my-commons")
        mock_client.search.assert_called_with(
            index="commons-index",
            body={
                "size": 0,
                "query": {
                    "constant_score": {
                        "filter": {"match": {"commons_name": "my-commons"}}
                    }
                },
                "aggs": {"_subjects_count": {"sum": {"field": "_subjects_count"}}},
            },
        )

    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.search",
        MagicMock(side_effect=Exception("some error")),
    ) as mock_search:
        assert await elasticsearch_dao.get_aggregations("my-commons") == []


@pytest.mark.asyncio
async def test_get_by_guid():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client", MagicMock()
    ) as mock_client:
        await elasticsearch_dao.get_by_guid("my-commons")
        mock_client.get.assert_called_with(
            index="commons-index",
            doc_type="commons",
            id="my-commons",
        )

    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.get",
        MagicMock(side_effect=Exception("some error")),
    ) as mock_get:
        assert await elasticsearch_dao.get_by_guid("my-commons") == None
