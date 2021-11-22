from elasticsearch import Elasticsearch, exceptions as es_exceptions
from typing import Any, List, Dict
import json
from mds import logger
from mds.config import AGG_MDS_NAMESPACE, ES_RETRY_LIMIT, ES_RETRY_INTERVAL

AGG_MDS_INDEX = f"{AGG_MDS_NAMESPACE}-commons-index"
AGG_MDS_TYPE = "commons"

AGG_MDS_INFO_INDEX = f"{AGG_MDS_NAMESPACE}-commons-info-index"
AGG_MDS_INFO_TYPE = "commons-info"

AGG_MDS_CONFIG_INDEX = f"{AGG_MDS_NAMESPACE}-commons-config-index"
AGG_MDS_CONFIG_TYPE = "commons-config"

# Setting Commons Info ES index to only store documents
# will not be searching on it
INFO_MAPPING = {
    "mappings": {
        AGG_MDS_INFO_TYPE: {
            "dynamic": False,
        }
    }
}

CONFIG = {
    "settings": {"index": {"number_of_shards": 1, "number_of_replicas": 0}},
    "mappings": {"_doc": {"properties": {"array": {"type": "keyword"}}}},
}

SEARCH_CONFIG = {
    "settings": {
        "index": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "analysis": {
                "tokenizer": {
                    "ngram_tokenizer": {
                        "type": "ngram",
                        "min_gram": 2,
                        "max_gram": 20,
                        "token_chars": ["letter", "digit"],
                    }
                },
                "analyzer": {
                    "ngram_analyzer": {
                        "type": "custom",
                        "tokenizer": "ngram_tokenizer",
                        "filter": ["lowercase"],
                    },
                    "search_analyzer": {
                        "type": "custom",
                        "tokenizer": "keyword",
                        "filter": "lowercase",
                    },
                },
            },
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
        timeout=ES_RETRY_INTERVAL,
        max_retries=ES_RETRY_LIMIT,
        retry_on_timeout=True,
    )


async def drop_all(common_mapping: dict):
    for index in [AGG_MDS_INDEX, AGG_MDS_INFO_INDEX, AGG_MDS_CONFIG_INDEX]:
        res = elastic_search_client.indices.delete(index=index, ignore=[400, 404])
        logger.debug(f"deleted index: {index}: {res}")

    try:
        mapping = {**SEARCH_CONFIG, **common_mapping}
        res = elastic_search_client.indices.create(index=AGG_MDS_INDEX, body=mapping)
        logger.debug(f"created index {AGG_MDS_INDEX}: {res}")
    except es_exceptions.RequestError as ex:
        if ex.error == "resource_already_exists_exception":
            logger.warning(f"index already exists: {AGG_MDS_INDEX}")
            pass  # Index already exists. Ignore.
        else:  # Other exception - raise it
            raise ex

    try:
        res = elastic_search_client.indices.create(
            index=AGG_MDS_INFO_INDEX, body=INFO_MAPPING
        )
        logger.debug(f"created index {AGG_MDS_INFO_INDEX}: {res}")

    except es_exceptions.RequestError as ex:
        if ex.error == "resource_already_exists_exception":
            logger.warning(f"index already exists: {AGG_MDS_INFO_INDEX}")
            pass  # Index already exists. Ignore.
        else:  # Other exception - raise it
            raise ex

    try:
        res = elastic_search_client.indices.create(
            index=AGG_MDS_CONFIG_INDEX, body=CONFIG
        )
        logger.debug(f"created index {AGG_MDS_CONFIG_INDEX}: {res}")
    except es_exceptions.RequestError as ex:
        if ex.error == "resource_already_exists_exception":
            logger.warning(f"index already exists: {AGG_MDS_CONFIG_INDEX}")
            pass  # Index already exists. Ignore.
        else:  # Other exception - raise it
            raise ex


def normalize_field(doc, key, normalize_type):
    try:
        if normalize_type == "object" and isinstance(doc[key], str):
            value = doc[key]
            doc[key] = None if value == "" else json.loads(value)
        if normalize_type == "number" and isinstance(doc[key], str):
            doc[key] = None
    except:
        logger.debug(f"error normalizing {key} for a document")
        doc[key] = None


async def update_metadata(
    name: str,
    data: List[Dict],
    guid_arr: List[str],
    tags: Dict[str, List[str]],
    info: Dict[str, str],
    study_data_field: str,
):
    elastic_search_client.index(
        index=AGG_MDS_INFO_INDEX,
        doc_type=AGG_MDS_INFO_TYPE,
        id=name,
        body=info,
    )

    for doc in data:
        key = list(doc.keys())[0]
        # Flatten out this structure
        doc = doc[key][study_data_field]

        try:
            elastic_search_client.index(
                index=AGG_MDS_INDEX, doc_type=AGG_MDS_TYPE, id=key, body=doc
            )
        except Exception as ex:
            print(ex)


async def update_global_info(key, doc) -> None:
    elastic_search_client.index(
        index=AGG_MDS_INFO_INDEX, doc_type=AGG_MDS_INFO_TYPE, id=key, body=doc
    )


async def update_config_info(doc) -> None:
    elastic_search_client.index(
        index=AGG_MDS_CONFIG_INDEX,
        doc_type="_doc",
        id=AGG_MDS_INDEX,
        body=doc,
    )


async def get_status():
    if not elastic_search_client.ping():
        raise ValueError("Connection failed")
    return "OK"


async def close():
    pass


async def get_commons():
    try:
        res = elastic_search_client.search(
            index=AGG_MDS_INDEX,
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


async def get_all_metadata(limit, offset, flatten=False):
    try:
        res = elastic_search_client.search(
            index=AGG_MDS_INDEX,
            body={"size": limit, "from": offset, "query": {"match_all": {}}},
        )
        if flatten:
            flat = []
            for record in res["hits"]["hits"]:
                id = record["_id"]
                normalized = record["_source"]
                flat.append(
                    {
                        id: {
                            "gen3_discovery": normalized,
                        }
                    }
                )
        else:
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
        res = elastic_search_client.search(
            index=AGG_MDS_INDEX,
            body={"query": {"match": {"commons_name.keyword": name}}},
        )
        return [x["_source"] for x in res["hits"]["hits"]]
    except Exception as error:
        logger.error(error)
        return {}


async def metadata_tags():
    try:
        res = elastic_search_client.search(
            index=AGG_MDS_INDEX,
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
        results = {}

        for info in res["aggregations"]["tags"]["categories"]["buckets"]:
            results[info["key"]] = {
                "total": info["doc_count"],
                "names": [{x["key"]: x["doc_count"] for x in info["name"]["buckets"]}],
            }

        return results

    except Exception as error:
        logger.error(error)
        return []


async def get_commons_attribute(name, what):
    try:
        data = elastic_search_client.search(
            index=AGG_MDS_INFO_INDEX,
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
            index=AGG_MDS_INDEX,
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


async def get_number_aggregation_for_field(field: str):
    try:
        # get the total number of documents in a commons namespace
        query = {
            "size": 0,
            "aggs": {
                field: {"sum": {"field": field}},
                "missing": {"missing": {"field": field}},
                "types_count": {"value_count": {"field": field}},
            },
        }
        nested = False
        parts = field.split(".")
        if len(parts) == 2:
            nested = True
            query["aggs"] = {
                field: {"nested": {"path": parts[0]}, "aggs": query["aggs"]}
            }

        res = elastic_search_client.search(index=AGG_MDS_INDEX, body=query)
        agg_results = res["aggregations"][field] if nested else res["aggregations"]

        return {
            field: {
                "total_items": res["hits"]["total"],
                "sum": agg_results[field]["value"],
                "missing": agg_results["missing"]["doc_count"],
            }
        }

    except Exception as error:
        logger.error(error)
        return {}


async def does_exists(field):
    try:
        query = {"size": 0, "query": {"bool": {"must": {"exists": {"field": field}}}}}
        res = elastic_search_client.search(index=AGG_MDS_INDEX, body=query)
        if res["hits"]["total"] > 0:
            return True
    except Exception as error:
        logger.error(error)
    return False


async def get_by_guid(guid):
    try:
        data = elastic_search_client.get(
            index=AGG_MDS_INDEX,
            doc_type=AGG_MDS_TYPE,
            id=guid,
        )
        return data["_source"]
    except Exception as error:
        logger.error(error)
        return None
