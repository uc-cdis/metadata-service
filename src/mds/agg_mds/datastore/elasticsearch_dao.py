from elasticsearch import Elasticsearch
from typing import List, Dict
from typing import Any
import json
from mds import logger


agg_mds_index = "commons-index"
agg_mds_type = "commons"


agg_mds_info_index = "commons-info-index"
agg_mds_info_type = "commons-info"


mapping = {
    "mappings": {
        "commons": {
            "properties": {
                "__manifest": {
                    "type": "nested",
                },
                "tags": {
                    "type": "nested",
                },
            }
        }
    }
}

elastic_search_client = None


async def init(hostname: str = "0.0.0.0", port: int = 9200):
    global elastic_search_client
    elastic_search_client = Elasticsearch(
        [hostname],
        scheme="http",
        port=port,
    )


async def drop_all():
    res = elastic_search_client.indices.delete(index="_all", ignore=[400, 404])
    logger.debug(f"deleted all indexes: {res}")
    res = elastic_search_client.indices.create(index=agg_mds_index, body=mapping)
    logger.debug(f"created index {agg_mds_index}: {res}")
    res = elastic_search_client.indices.create(
        index=agg_mds_info_index,
    )
    logger.debug(f"created index {agg_mds_info_index}: {res}")


def normalize_string_or_object(doc, key):
    if key in doc and isinstance(doc[key], str):
        manifest = doc[key]
        doc[key] = None if manifest is "" else json.loads(manifest)


async def update_metadata(
    name: str,
    data: List[Dict],
    guid_arr: List[str],
    tags: Dict[str, List[str]],
    info: Dict[str, str],
):
    elastic_search_client.index(
        index=agg_mds_info_index,
        doc_type=agg_mds_info_type,
        id=name,
        body=info,
    )

    for doc in data:
        key = list(doc.keys())[0]
        # Flatten out this structure
        doc = doc[key]["gen3_discovery"]

        normalize_string_or_object(doc, "__manifest")
        # TODO WFH Why do we have this redundant field? Which commons has this?
        normalize_string_or_object(doc, "_file_manifest")
        normalize_string_or_object(doc, "advSearchFilters")
        elastic_search_client.index(
            index=agg_mds_index, doc_type=agg_mds_type, id=key, body=doc
        )


async def get_status():
    return elastic_search_client.cluster.health()


async def close():
    pass


async def get_commons():
    try:
        res = elastic_search_client.search(
            index=agg_mds_index,
            body={
                "size": 0,
                "aggs": {"commons_names": {"terms": {"field": "commons_name.keyword"}}},
            },
        )
        return {
            "commons": [
                x["key"] for x in res["aggregations"]["commons_names"]["buckets"]
            ]
        }
    except Exception as error:
        logger.error(error)
        return []


async def get_all_metadata(limit, offset):
    try:
        res = elastic_search_client.search(
            index=agg_mds_index,
            body={"size": limit, "from": offset, "query": {"match_all": {}}},
        )
        byCommons = {}
        for record in res["hits"]["hits"]:
            id = record["_id"]
            normalized = record["_source"]
            commons_name = normalized["commons_name"]
            if commons_name not in byCommons:
                byCommons[commons_name] = []
            byCommons[commons_name].append(
                {
                    id: {
                        "gen3_discovery": normalized,
                    }
                }
            )
        return byCommons
    except Exception as error:
        logger.error(error)
        return {}


async def get_all_named_commons_metadata(name):
    try:
        return elastic_search_client.search(
            index=agg_mds_index,
            body={"query": {"match": {"commons_name.keyword": name}}},
        )
    except Exception as error:
        logger.error(error)
        return {}


async def metadata_tags(name):
    try:
        return elastic_search_client.search(
            index=agg_mds_index,
            body={
                "size": 0,
                "aggs": {
                    "tags": {
                        "nested": {"path": "tags"},
                        "aggs": {
                            "categories": {
                                "terms": {"field": "tags.category.keyword"},
                                "aggs": {
                                    "name": {
                                        "terms": {
                                            "field": "tags.name.keyword",
                                        }
                                    }
                                },
                            }
                        },
                    }
                },
            },
        )
    except Exception as error:
        logger.error(error)
        return []


async def get_commons_attribute(name, what):
    try:
        data = elastic_search_client.search(
            index=agg_mds_info_index,
            body={
                "query": {
                    "terms": {
                        "_id": [name],
                    }
                }
            },
        )
        return data["hits"]["hits"][0]["_source"]
    except Exception as error:
        logger.error(error)
        return None


async def get_aggregations(name):
    try:
        res = elastic_search_client.search(
            index=agg_mds_index,
            body={
                "size": 0,
                "query": {
                    "constant_score": {
                        "filter": {
                            "match": {"commons_name": name},
                        }
                    }
                },
                "aggs": {"_subjects_count": {"sum": {"field": "_subjects_count"}}},
            },
        )
        return {
            "_subjects_count": res["aggregations"]["_subjects_count"]["value"],
        }
    except Exception as error:
        logger.error(error)
        return []


async def get_by_guid(guid):
    try:
        data = elastic_search_client.get(
            index=agg_mds_index,
            doc_type=agg_mds_type,
            id=guid,
        )
        return data["_source"]
    except Exception as error:
        logger.error(error)
        return None
