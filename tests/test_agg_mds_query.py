from typing import Dict
import pytest
import nest_asyncio
from mds.agg_mds import datastore
from unittest.mock import patch, MagicMock
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
        datastore, "get_all_metadata", AsyncMock(return_value={"results": []})
    ) as datastore_mock:
        resp = client.get("/aggregate/metadata")
        assert resp.status_code == 200
        assert resp.json() == []
        datastore.get_all_metadata.assert_called_with(20, 0, "", False)

    mock_data = {
        "results": {
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
    }

    with patch.object(
        datastore, "get_all_metadata", AsyncMock(return_value=mock_data)
    ) as datastore_mock:
        resp = client.get("/aggregate/metadata")
        assert resp.status_code == 200
        assert resp.json() == mock_data["results"]
        datastore.get_all_metadata.assert_called_with(20, 0, "", False)


@pytest.mark.asyncio
async def test_aggregate_metadata_paged(client):
    with patch.object(
        datastore, "get_all_metadata", AsyncMock(return_value={"results": []})
    ) as datastore_mock:
        resp = client.get("/aggregate/metadata?pagination=1&flatten=1")
        assert resp.status_code == 200
        assert resp.json() == {"results": []}
        datastore.get_all_metadata.assert_called_with(20, 0, "", True)

    mock_data = {
        "results": [
            {"study1": {}},
            {"study2": {}},
        ],
        "pagination": {"hits": 64, "offset": 0, "pageSize": 20, "pages": 4},
    }

    with patch.object(
        datastore, "get_all_metadata", AsyncMock(return_value=mock_data)
    ) as datastore_mock:
        resp = client.get("/aggregate/metadata?pagination=1&flatten=1")
        assert resp.status_code == 200
        assert resp.json() == mock_data
        datastore.get_all_metadata.assert_called_with(20, 0, "", True)


@pytest.mark.asyncio
async def test_aggregate_metadata_paged_flat(client):
    mock_data = {
        "took": 3,
        "timed_out": "false",
        "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
        "hits": {
            "total": 161,
            "max_score": 1.0,
            "hits": [
                {
                    "_index": "default_namespace-commons-index",
                    "_type": "commons",
                    "_id": "815616c0-dfsdfjjj",
                    "_score": 1.0,
                    "_source": {
                        "gen3_discovery": {
                            "link": "",
                            "tags": [
                                {"name": "restricted", "category": "Access"},
                                {"name": "genomic", "category": "category"},
                            ],
                            "commons": "LI",
                            "_unique_id": "815616c0-c4a4-4883-9107-a05694499a36",
                            "dataset_code": "LI",
                            "brief_summary": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
                            "dataset_title": "Lorem ipsum dolor sit amet",
                            "samples_count": "",
                            "subjects_count": "",
                            "data_files_count": 11062,
                            "_subjects_count": "",
                            "study_description": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ultricies tristique nulla aliquet enim tortor at auctor.",
                            "short_name": "Lorem ipsum dolor sit amet",
                            "full_name": "Lorem ipsum dolor sit amet, consectetur adipiscing elit",
                            "commons_name": "Lorem ipsum",
                            "__manifest": [
                                {"filename": "foo2.txt"},
                                {"filename": "foo3.txt"},
                            ],
                        },
                    },
                }
            ],
        },
    }

    results = {
        "pagination": {"hits": 161, "offset": 0, "pageSize": 20, "pages": 9},
        "results": [
            {
                "815616c0-dfsdfjjj": {
                    "gen3_discovery": {
                        "link": "",
                        "tags": [
                            {"name": "restricted", "category": "Access"},
                            {"name": "genomic", "category": "category"},
                        ],
                        "commons": "LI",
                        "_unique_id": "815616c0-c4a4-4883-9107-a05694499a36",
                        "dataset_code": "LI",
                        "brief_summary": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
                        "dataset_title": "Lorem ipsum dolor sit amet",
                        "samples_count": "",
                        "subjects_count": "",
                        "data_files_count": 11062,
                        "_subjects_count": "",
                        "study_description": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ultricies tristique nulla aliquet enim tortor at auctor.",
                        "short_name": "Lorem ipsum dolor sit amet",
                        "full_name": "Lorem ipsum dolor sit amet, consectetur adipiscing elit",
                        "commons_name": "Lorem ipsum",
                        "__manifest": [
                            {"filename": "foo2.txt"},
                            {"filename": "foo3.txt"},
                        ],
                    }
                }
            }
        ],
    }

    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.search",
        MagicMock(return_value=mock_data),
    ) as search:
        resp = client.get("/aggregate/metadata?flatten=true&pagination=true")
        assert resp.status_code == 200
        assert resp.json() == results


@pytest.mark.asyncio
async def test_aggregate_metadata_counts(client):
    mock_data = {
        "took": 3,
        "timed_out": "false",
        "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
        "hits": {
            "total": 161,
            "max_score": 1.0,
            "hits": [
                {
                    "_index": "default_namespace-commons-index",
                    "_type": "commons",
                    "_id": "815616c0-dfsdfjjj",
                    "_score": 1.0,
                    "_source": {
                        "gen3_discovery": {
                            "link": "",
                            "tags": [
                                {"name": "restricted", "category": "Access"},
                                {"name": "genomic", "category": "category"},
                            ],
                            "commons": "LI",
                            "_unique_id": "815616c0-c4a4-4883-9107-a05694499a36",
                            "dataset_code": "LI",
                            "brief_summary": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
                            "dataset_title": "Lorem ipsum dolor sit amet",
                            "samples_count": "",
                            "subjects_count": "",
                            "data_files_count": 11062,
                            "_subjects_count": "",
                            "study_description": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ultricies tristique nulla aliquet enim tortor at auctor.",
                            "short_name": "Lorem ipsum dolor sit amet",
                            "full_name": "Lorem ipsum dolor sit amet, consectetur adipiscing elit",
                            "commons_name": "Lorem ipsum",
                            "__manifest": [
                                {"filename": "foo2.txt"},
                                {"filename": "foo3.txt"},
                            ],
                        },
                    },
                }
            ],
        },
    }

    results = {
        "Lorem ipsum": [
            {
                "815616c0-dfsdfjjj": {
                    "gen3_discovery": {
                        "link": "",
                        "tags": [
                            {"name": "restricted", "category": "Access"},
                            {"name": "genomic", "category": "category"},
                        ],
                        "commons": "LI",
                        "_unique_id": "815616c0-c4a4-4883-9107-a05694499a36",
                        "dataset_code": "LI",
                        "brief_summary": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
                        "dataset_title": "Lorem ipsum dolor sit amet",
                        "samples_count": "",
                        "subjects_count": "",
                        "data_files_count": 11062,
                        "_subjects_count": "",
                        "study_description": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ultricies tristique nulla aliquet enim tortor at auctor.",
                        "short_name": "Lorem ipsum dolor sit amet",
                        "full_name": "Lorem ipsum dolor sit amet, consectetur adipiscing elit",
                        "commons_name": "Lorem ipsum",
                        "__manifest": 2,
                    }
                }
            }
        ]
    }

    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.search",
        MagicMock(return_value=mock_data),
    ) as search:
        resp = client.get("/aggregate/metadata?counts=__manifest")
        assert resp.status_code == 200
        assert resp.json() == results

    # test multiple counts field
    mock_data["hits"]["hits"][0]["_source"]["gen3_discovery"]["__manifest"] = [
        {"filename": "foo2.txt"},
        {"filename": "foo3.txt"},
    ]
    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.search",
        MagicMock(return_value=mock_data),
    ) as search:
        results["Lorem ipsum"][0]["815616c0-dfsdfjjj"]["gen3_discovery"]["tags"] = 2
        resp = client.get("/aggregate/metadata?counts=__manifest,tags")
        assert resp.status_code == 200
        assert resp.json() == results


@pytest.mark.asyncio
async def test_aggregate_metadata_counts_null(client):
    mock_data = {
        "took": 3,
        "timed_out": "false",
        "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
        "hits": {
            "total": 161,
            "max_score": 1.0,
            "hits": [
                {
                    "_index": "default_namespace-commons-index",
                    "_type": "commons",
                    "_id": "815616c0-dfsdfjjj",
                    "_score": 1.0,
                    "_source": {
                        "gen3_discovery": {
                            "link": "",
                            "tags": [
                                {"name": "restricted", "category": "Access"},
                                {"name": "genomic", "category": "category"},
                            ],
                            "commons": "LI",
                            "_unique_id": "815616c0-c4a4-4883-9107-a05694499a36",
                            "dataset_code": "LI",
                            "brief_summary": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
                            "dataset_title": "Lorem ipsum dolor sit amet",
                            "samples_count": "",
                            "subjects_count": "",
                            "data_files_count": 11062,
                            "_subjects_count": "",
                            "study_description": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ultricies tristique nulla aliquet enim tortor at auctor.",
                            "short_name": "Lorem ipsum dolor sit amet",
                            "full_name": "Lorem ipsum dolor sit amet, consectetur adipiscing elit",
                            "commons_name": "Lorem ipsum",
                            "__manifest": None,
                        },
                    },
                }
            ],
        },
    }

    results = {
        "Lorem ipsum": [
            {
                "815616c0-dfsdfjjj": {
                    "gen3_discovery": {
                        "link": "",
                        "tags": [
                            {"name": "restricted", "category": "Access"},
                            {"name": "genomic", "category": "category"},
                        ],
                        "commons": "LI",
                        "_unique_id": "815616c0-c4a4-4883-9107-a05694499a36",
                        "dataset_code": "LI",
                        "brief_summary": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
                        "dataset_title": "Lorem ipsum dolor sit amet",
                        "samples_count": "",
                        "subjects_count": "",
                        "data_files_count": 11062,
                        "_subjects_count": "",
                        "study_description": "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ultricies tristique nulla aliquet enim tortor at auctor.",
                        "short_name": "Lorem ipsum dolor sit amet",
                        "full_name": "Lorem ipsum dolor sit amet, consectetur adipiscing elit",
                        "commons_name": "Lorem ipsum",
                        "__manifest": 0,
                    }
                }
            }
        ]
    }

    with patch(
        "mds.agg_mds.datastore.elasticsearch_dao.elastic_search_client.search",
        MagicMock(return_value=mock_data),
    ) as search:
        resp = client.get("/aggregate/metadata?counts=__manifest")
        assert resp.status_code == 200
        assert resp.json() == results


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
        datastore, "get_all_tags", AsyncMock(return_value={})
    ) as datastore_mock:
        resp = client.get("/aggregate/tags")
        assert resp.status_code == 404
        assert resp.json() == {
            "detail": {
                "code": 404,
                "message": "error retrieving tags from service",
            }
        }

    tags = {
        "Access": {"total": 63, "names": [{"restricted": 63}]},
        "Category": {
            "total": 61,
            "names": [
                {
                    "Family/Twin/Trios": 39,
                    "Prospective Longitudinal Cohort": 10,
                    "Tumor vs. Matched-Normal": 9,
                    "Cross-Sectional": 3,
                }
            ],
        },
    }

    with patch.object(
        datastore, "get_all_tags", AsyncMock(return_value=tags)
    ) as datastore_mock:
        resp = client.get("/aggregate/tags")
        assert resp.status_code == 200
        assert resp.json() == tags
        datastore.get_all_tags.assert_called_with()


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
        datastore.get_commons_attribute.assert_called_with("commons1")

    with patch.object(
        datastore,
        "get_commons_attribute",
        AsyncMock(return_value={"commons_url": "http://commons"}),
    ) as datastore_mock:
        resp = client.get("/aggregate/metadata/commons1/info")
        assert resp.status_code == 200
        assert resp.json() == {"commons_url": "http://commons"}
        datastore.get_commons_attribute.assert_called_with("commons1")


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


@pytest.mark.asyncio
async def test_aggregate_metadata_get_schema(client):
    schema = {
        "_subjects_count": {"type": "integer", "description": ""},
        "year_awarded": {"type": "integer", "description": ""},
    }
    with patch.object(
        datastore,
        "get_commons_attribute",
        AsyncMock(
            return_value={
                "_subjects_count": {"type": "integer", "description": ""},
                "year_awarded": {"type": "integer", "description": ""},
            }
        ),
    ):
        resp = client.get("/aggregate/info/schema")
        assert resp.status_code == 200
        assert resp.json() == schema
        datastore.get_commons_attribute.assert_called_with("schema")

    with patch.object(
        datastore,
        "get_commons_attribute",
        AsyncMock(return_value=None),
    ) as datastore_mock:
        # test for unknown info string
        resp = client.get("/aggregate/info/nothing")
        assert resp.status_code == 404
        assert resp.json() == {
            "detail": {
                "code": 404,
                "message": "information for nothing not found",
            }
        }
