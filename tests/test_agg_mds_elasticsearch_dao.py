from unittest.mock import patch, call, MagicMock
import pytest
from mds.agg_mds.datastore import elasticsearch_dao
from mds.agg_mds.datastore.elasticsearch_dao import (
    INFO_MAPPING,
    AGG_MDS_INDEX,
    AGG_MDS_INFO_INDEX,
    AGG_MDS_CONFIG_INDEX,
    CONFIG,
    SEARCH_CONFIG,
    AGG_MDS_INDEX_TEMP,
    AGG_MDS_INFO_INDEX_TEMP,
    AGG_MDS_CONFIG_INDEX_TEMP,
    AGG_MDS_INFO_TYPE,
    AGG_MDS_DEFAULT_STUDY_DATA_FIELD,
    count,
    process_record,
)
from opensearchpy import exceptions as os_exceptions
from mds.config import ES_RETRY_LIMIT, ES_RETRY_INTERVAL

COMMON_MAPPING = {
    "mappings": {
        "properties": {
            "__manifest": {
                "type": "nested",
            },
            "tags": {
                "type": "nested",
            },
            "data_dictionary": {
                "type": "nested",
            },
        }
    }
}


@pytest.mark.asyncio
async def test_init():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.OpenSearch", MagicMock()
    ) as mock_client:
        await elasticsearch_dao.init("myhost")
    mock_client.assert_called_with(
        hosts=["myhost:9200"],
        timeout=ES_RETRY_INTERVAL,
        max_retries=ES_RETRY_LIMIT,
        retry_on_timeout=True,
    )


@pytest.mark.asyncio
async def test_drop_all_non_temp_indexes():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.indices",
        MagicMock(),
    ) as mock_indices:
        await elasticsearch_dao.drop_all_non_temp_indexes()
    mock_indices.delete.assert_has_calls(
        [
            call(index=AGG_MDS_INDEX, ignore=[400, 404]),
            call(index=AGG_MDS_INFO_INDEX, ignore=[400, 404]),
            call(index=AGG_MDS_CONFIG_INDEX, ignore=[400, 404]),
        ],
        any_order=True,
    )


@pytest.mark.asyncio
async def test_drop_all_temp_indexes():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.indices",
        MagicMock(),
    ) as mock_indices:
        await elasticsearch_dao.drop_all_temp_indexes()
    mock_indices.delete.assert_has_calls(
        [
            call(index=AGG_MDS_INDEX_TEMP, ignore=[400, 404]),
            call(index=AGG_MDS_INFO_INDEX_TEMP, ignore=[400, 404]),
            call(index=AGG_MDS_CONFIG_INDEX_TEMP, ignore=[400, 404]),
        ],
        any_order=True,
    )


@pytest.mark.asyncio
async def test_clone_temp_indexes_to_real_indexes():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client",
        MagicMock(),
    ) as mock_client:
        await elasticsearch_dao.clone_temp_indexes_to_real_indexes()
    mock_client.reindex.assert_has_calls(
        [
            call(
                body={
                    "source": {"index": AGG_MDS_INDEX_TEMP},
                    "dest": {"index": AGG_MDS_INDEX},
                },
            ),
            call(
                body={
                    "source": {"index": AGG_MDS_INFO_INDEX_TEMP},
                    "dest": {"index": AGG_MDS_INFO_INDEX},
                },
            ),
            call(
                body={
                    "source": {"index": AGG_MDS_CONFIG_INDEX_TEMP},
                    "dest": {"index": AGG_MDS_CONFIG_INDEX},
                },
            ),
        ],
        any_order=True,
    )


@pytest.mark.asyncio
async def test_create_indexes():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.indices",
        MagicMock(),
    ) as mock_indices:
        await elasticsearch_dao.create_indexes(common_mapping=COMMON_MAPPING)
    mock_indices.create.assert_has_calls(
        [
            call(body={**SEARCH_CONFIG, **COMMON_MAPPING}, index=AGG_MDS_INDEX),
            call(body=INFO_MAPPING, index=AGG_MDS_INFO_INDEX),
            call(body=CONFIG, index=AGG_MDS_CONFIG_INDEX),
        ],
        any_order=True,
    )


@pytest.mark.asyncio
async def test_create_indexes():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.indices",
        MagicMock(),
    ) as mock_indices:
        await elasticsearch_dao.create_indexes(common_mapping=COMMON_MAPPING)
    mock_indices.create.assert_has_calls(
        [
            call(body={**SEARCH_CONFIG, **COMMON_MAPPING}, index=AGG_MDS_INDEX),
            call(body=INFO_MAPPING, index=AGG_MDS_INFO_INDEX),
            call(body=CONFIG, index=AGG_MDS_CONFIG_INDEX),
        ],
        any_order=True,
    )


@pytest.mark.asyncio
async def test_create_temp_indexes():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.indices",
        MagicMock(),
    ) as mock_indices:
        await elasticsearch_dao.create_temp_indexes(common_mapping=COMMON_MAPPING)
    mock_indices.create.assert_has_calls(
        [
            call(body={**SEARCH_CONFIG, **COMMON_MAPPING}, index=AGG_MDS_INDEX_TEMP),
            call(body=INFO_MAPPING, index=AGG_MDS_INFO_INDEX_TEMP),
            call(body=CONFIG, index=AGG_MDS_CONFIG_INDEX_TEMP),
        ],
        any_order=True,
    )


@pytest.mark.asyncio
async def test_create_if_exists():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.indices.create",
        MagicMock(
            side_effect=os_exceptions.RequestError(
                400, "resource_already_exists_exception"
            )
        ),
    ):
        await elasticsearch_dao.drop_all_non_temp_indexes()
        await elasticsearch_dao.create_indexes(common_mapping=COMMON_MAPPING)


@pytest.mark.asyncio
async def test_create_index_raise_exception():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.indices.create",
        MagicMock(side_effect=os_exceptions.RequestError(403, "expect_to_fail")),
    ):
        try:
            await elasticsearch_dao.create_indexes(common_mapping=COMMON_MAPPING)
        except Exception as exc:
            assert isinstance(exc, os_exceptions.RequestError) is True


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
                        AGG_MDS_DEFAULT_STUDY_DATA_FIELD: {
                            "some_field": "some_value",
                            "__manifest": {},
                            "sites": "",
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
                id="my_commons",
                index=AGG_MDS_INFO_INDEX,
            ),
            call(
                body={
                    AGG_MDS_DEFAULT_STUDY_DATA_FIELD: {
                        "some_field": "some_value",
                        "__manifest": {},
                        "sites": "",
                    }
                },
                id="my_id",
                index=AGG_MDS_INDEX,
            ),
        ],
        any_order=True,
    )


@pytest.mark.asyncio
async def test_update_metadata_to_temp_index():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.index",
        MagicMock(),
    ) as mock_index:
        await elasticsearch_dao.update_metadata(
            "my_commons",
            [
                {
                    "my_id": {
                        AGG_MDS_DEFAULT_STUDY_DATA_FIELD: {
                            "some_field": "some_value",
                            "__manifest": {},
                            "sites": "",
                        }
                    }
                }
            ],
            [],
            {},
            {},
            use_temp_index=True,
        )
    mock_index.assert_has_calls(
        [
            call(
                body={},
                id="my_commons",
                index=AGG_MDS_INFO_INDEX_TEMP,
            ),
            call(
                body={
                    AGG_MDS_DEFAULT_STUDY_DATA_FIELD: {
                        "some_field": "some_value",
                        "__manifest": {},
                        "sites": "",
                    }
                },
                id="my_id",
                index=AGG_MDS_INDEX_TEMP,
            ),
        ],
        any_order=True,
    )


@pytest.mark.asyncio
async def test_update_global_info():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client",
        MagicMock(),
    ) as mock_client:
        await elasticsearch_dao.update_global_info(key="123", doc={})

    mock_client.index.assert_called_with(index=AGG_MDS_INFO_INDEX, id="123", body={})


@pytest.mark.asyncio
async def test_update_global_info_to_temp_index():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client",
        MagicMock(),
    ) as mock_client:
        await elasticsearch_dao.update_global_info(
            key="123", doc={}, use_temp_index=True
        )

    mock_client.index.assert_called_with(
        index=AGG_MDS_INFO_INDEX_TEMP, id="123", body={}
    )


@pytest.mark.asyncio
async def test_update_config_info():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client",
        MagicMock(),
    ) as mock_client:
        await elasticsearch_dao.update_config_info(doc={})

    mock_client.index.assert_called_with(
        index=AGG_MDS_CONFIG_INDEX, id=AGG_MDS_INDEX, body={}
    )


@pytest.mark.asyncio
async def test_update_config_info_to_temp_index():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client",
        MagicMock(),
    ) as mock_client:
        await elasticsearch_dao.update_config_info(doc={}, use_temp_index=True)

    mock_client.index.assert_called_with(
        index=AGG_MDS_CONFIG_INDEX_TEMP, id=AGG_MDS_INDEX, body={}
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
async def close():
    assert True


@pytest.mark.asyncio
async def test_get_commons():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.search",
        MagicMock(),
    ) as mock_search:
        await elasticsearch_dao.get_commons()
        mock_search.assert_called_with(
            index=AGG_MDS_INDEX,
            body={
                "size": 0,
                "aggs": {
                    AGG_MDS_DEFAULT_STUDY_DATA_FIELD: {
                        "nested": {"path": AGG_MDS_DEFAULT_STUDY_DATA_FIELD},
                        "aggs": {
                            "commons_names": {
                                "terms": {
                                    "field": f"{AGG_MDS_DEFAULT_STUDY_DATA_FIELD}.commons_name.keyword"
                                }
                            }
                        },
                    }
                },
            },
        )

    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.search",
        MagicMock(side_effect=Exception("some error")),
    ):
        assert await elasticsearch_dao.get_commons() == []


def test_count_dict():
    assert count({1: 2, 3: 4}) == 2


def test_count_list():
    assert count([1, 2, 3]) == 3


def test_count_value_number():
    assert count(123) == 123


def test_count_value_string():
    assert count("imastring") == "imastring"


def test_count_value_none():
    assert count(None) == 0


def test_process_records():
    _id = "123"
    _source = {
        AGG_MDS_DEFAULT_STUDY_DATA_FIELD: {"count": [1, 2, 3, 4], "name": "my_name"}
    }
    record = {"_id": _id, "_source": _source}
    rid, normalized = process_record(record, ["count"])
    assert rid == _id
    assert normalized == {
        AGG_MDS_DEFAULT_STUDY_DATA_FIELD: {"count": 4, "name": "my_name"}
    }

    # test if passed dict field is not array
    rid, normalized = process_record(record, ["name"])
    assert rid == _id
    assert normalized == _source


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
            index=AGG_MDS_INDEX,
            body={"size": 5, "from": 9, "query": {"match_all": {}}},
        )

    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.search",
        MagicMock(side_effect=Exception("some error")),
    ):
        assert await elasticsearch_dao.get_all_metadata(5, 9) == {}


@pytest.mark.asyncio
async def test_get_all_named_commons_metadata():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client", MagicMock()
    ) as mock_client:
        await elasticsearch_dao.get_all_named_commons_metadata("my-commons")
        mock_client.search.assert_called_with(
            index=AGG_MDS_INDEX,
            body={
                "query": {
                    "nested": {
                        "path": AGG_MDS_DEFAULT_STUDY_DATA_FIELD,
                        "query": {
                            "match": {
                                f"{AGG_MDS_DEFAULT_STUDY_DATA_FIELD}.commons_name.keyword": "HEAL"
                            }
                        },
                    }
                }
            },
        )

    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.search",
        MagicMock(side_effect=Exception("some error")),
    ):
        assert (
            await elasticsearch_dao.get_all_named_commons_metadata("my-commons") == {}
        )


@pytest.mark.asyncio
async def test_metadata_tags():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client", MagicMock()
    ) as mock_client:
        await elasticsearch_dao.metadata_tags()
        mock_client.search.assert_called_with(
            index=AGG_MDS_INDEX,
            body={
                "size": 0,
                "aggs": {
                    "tags": {
                        "nested": {"path": f"{AGG_MDS_DEFAULT_STUDY_DATA_FIELD}.tags"},
                        "aggs": {
                            "categories": {
                                "terms": {
                                    "field": f"{AGG_MDS_DEFAULT_STUDY_DATA_FIELD}.tags.category.keyword"
                                },
                                "aggs": {
                                    "name": {
                                        "terms": {
                                            "field": f"{AGG_MDS_DEFAULT_STUDY_DATA_FIELD}.tags.name.keyword"
                                        }
                                    }
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
    ):
        assert await elasticsearch_dao.metadata_tags() == []


@pytest.mark.asyncio
async def test_get_commons_attribute():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client", MagicMock()
    ) as mock_client:
        await elasticsearch_dao.get_commons_attribute("my-commons")
        mock_client.search.assert_called_with(
            index=AGG_MDS_INFO_INDEX,
            body={"query": {"terms": {"_id": ["my-commons"]}}},
        )

    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.search",
        MagicMock(side_effect=Exception("some error")),
    ):
        assert await elasticsearch_dao.get_commons_attribute("my-commons") is None


@pytest.mark.asyncio
async def test_get_aggregations():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client", MagicMock()
    ) as mock_client:
        await elasticsearch_dao.get_aggregations("my-commons")
        mock_client.search.assert_called_with(
            index=AGG_MDS_INDEX,
            body={
                "size": 0,
                "query": {
                    "constant_score": {
                        "filter": {
                            "match": {
                                f"{AGG_MDS_DEFAULT_STUDY_DATA_FIELD}.commons_name": "my-commons"
                            }
                        }
                    }
                },
                "aggs": {"_subjects_count": {"sum": {"field": "_subjects_count"}}},
            },
        )

    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.search",
        MagicMock(side_effect=Exception("some error")),
    ):
        assert await elasticsearch_dao.get_aggregations("my-commons") == []


@pytest.mark.asyncio
async def test_get_by_guid():
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client", MagicMock()
    ) as mock_client:
        await elasticsearch_dao.get_by_guid("my-commons")
        mock_client.get.assert_called_with(
            index=AGG_MDS_INDEX,
            id="my-commons",
        )

    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.get",
        MagicMock(side_effect=Exception("some error")),
    ):
        assert await elasticsearch_dao.get_by_guid("my-commons") is None
