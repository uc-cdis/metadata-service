from elasticsearch import Elasticsearch, exceptions as es_exceptions, helpers
from typing import Any, List, Dict, Union, Optional, Tuple
from math import ceil
from mds import logger
from mds.config import (
    AGG_MDS_NAMESPACE,
    ES_RETRY_LIMIT,
    ES_RETRY_INTERVAL,
    AGG_MDS_DEFAULT_STUDY_DATA_FIELD,
)

AGG_MDS_INDEX = f"{AGG_MDS_NAMESPACE}-commons-index"
AGG_MDS_TYPE = "commons"
AGG_MDS_INDEX_TEMP = f"{AGG_MDS_NAMESPACE}-commons-index-temp"

AGG_MDS_INFO_INDEX = f"{AGG_MDS_NAMESPACE}-commons-info-index"
AGG_MDS_INFO_TYPE = "commons-info"
AGG_MDS_INFO_INDEX_TEMP = f"{AGG_MDS_NAMESPACE}-commons-info-index-temp"

AGG_MDS_CONFIG_INDEX = f"{AGG_MDS_NAMESPACE}-commons-config-index"
AGG_MDS_CONFIG_TYPE = "commons-config"
AGG_MDS_CONFIG_INDEX_TEMP = f"{AGG_MDS_NAMESPACE}-commons-config-index-temp"

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
            "mapping.ignore_malformed": True,
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "analysis": {
                "tokenizer": {
                    "ngram_tokenizer": {
                        "type": "ngram",
                        "min_gram": 3,
                        "max_gram": 3,
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


async def drop_all_non_temp_indexes():
    for index in [AGG_MDS_INDEX, AGG_MDS_INFO_INDEX, AGG_MDS_CONFIG_INDEX]:
        res = elastic_search_client.indices.delete(index=index, ignore=[400, 404])
        logger.debug(f"deleted index: {index}: {res}")


async def drop_all_temp_indexes():
    for index in [
        AGG_MDS_INDEX_TEMP,
        AGG_MDS_INFO_INDEX_TEMP,
        AGG_MDS_CONFIG_INDEX_TEMP,
    ]:
        res = elastic_search_client.indices.delete(index=index, ignore=[400, 404])
        logger.debug(f"deleted index: {index}: {res}")


async def clone_temp_indexes_to_real_indexes():
    for index in [AGG_MDS_INDEX, AGG_MDS_INFO_INDEX, AGG_MDS_CONFIG_INDEX]:
        source_index = index + "-temp"
        reqBody = {"source": {"index": source_index}, "dest": {"index": index}}
        logger.debug(f"Cloning index: {source_index} to {index}...")
        res = Elasticsearch.reindex(elastic_search_client, reqBody)
        # Elasticsearch >7.4 introduces the clone api we could use later on
        # res = elastic_search_client.indices.clone(index=source_index, target=index)
        logger.debug(f"Cloned index: {source_index} to {index}: {res}")


async def create_indexes(common_mapping: dict):
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


async def create_temp_indexes(common_mapping: dict):
    try:
        mapping = {**SEARCH_CONFIG, **common_mapping}
        res = elastic_search_client.indices.create(
            index=AGG_MDS_INDEX_TEMP, body=mapping
        )
        logger.debug(f"created index {AGG_MDS_INDEX_TEMP}: {res}")
    except es_exceptions.RequestError as ex:
        if ex.error == "resource_already_exists_exception":
            logger.warning(f"index already exists: {AGG_MDS_INDEX_TEMP}")
            pass  # Index already exists. Ignore.
        else:  # Other exception - raise it
            raise ex

    try:
        res = elastic_search_client.indices.create(
            index=AGG_MDS_INFO_INDEX_TEMP, body=INFO_MAPPING
        )
        logger.debug(f"created index {AGG_MDS_INFO_INDEX_TEMP}: {res}")

    except es_exceptions.RequestError as ex:
        if ex.error == "resource_already_exists_exception":
            logger.warning(f"index already exists: {AGG_MDS_INFO_INDEX_TEMP}")
            pass  # Index already exists. Ignore.
        else:  # Other exception - raise it
            raise ex

    try:
        res = elastic_search_client.indices.create(
            index=AGG_MDS_CONFIG_INDEX_TEMP, body=CONFIG
        )
        logger.debug(f"created index {AGG_MDS_CONFIG_INDEX_TEMP}: {res}")
    except es_exceptions.RequestError as ex:
        if ex.error == "resource_already_exists_exception":
            logger.warning(f"index already exists: {AGG_MDS_CONFIG_INDEX_TEMP}")
            pass  # Index already exists. Ignore.
        else:  # Other exception - raise it
            raise ex


async def update_metadata(
    name: str,
    data: List[Dict],
    guid_arr: List[str],
    tags: Dict[str, List[str]],
    info: Dict[str, str],
    data_dict_field: str = None,
    use_temp_index: bool = False,
):
    index_to_update = AGG_MDS_INFO_INDEX_TEMP if use_temp_index else AGG_MDS_INFO_INDEX
    elastic_search_client.index(
        index=index_to_update,
        doc_type=AGG_MDS_INFO_TYPE,
        id=name,
        body=info,
    )

    index_to_update = AGG_MDS_INDEX_TEMP if use_temp_index else AGG_MDS_INDEX
    for d in data:
        key = list(d.keys())[0]
        # Flatten out this structure
        doc = {
            AGG_MDS_DEFAULT_STUDY_DATA_FIELD: d[key][AGG_MDS_DEFAULT_STUDY_DATA_FIELD]
        }
        if data_dict_field in d[key]:
            doc[data_dict_field] = d[key][data_dict_field]
        print(doc)

        try:
            elastic_search_client.index(
                index=index_to_update, doc_type=AGG_MDS_TYPE, id=key, body=doc
            )
        except Exception as ex:
            raise (ex)


async def update_global_info(key, doc, use_temp_index: bool = False) -> None:
    index_to_update = AGG_MDS_INFO_INDEX_TEMP if use_temp_index else AGG_MDS_INFO_INDEX
    elastic_search_client.index(
        index=index_to_update, doc_type=AGG_MDS_INFO_TYPE, id=key, body=doc
    )


async def update_config_info(doc, use_temp_index: bool = False) -> None:
    index_to_update = (
        AGG_MDS_CONFIG_INDEX_TEMP if use_temp_index else AGG_MDS_CONFIG_INDEX
    )
    elastic_search_client.index(
        index=index_to_update,
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


def count(value) -> Union[int, Any]:
    """
    Returns the length of the value if list or dict otherwise returns the value
    If value is None returns 0
    """
    if value is None:
        return 0
    if isinstance(value, dict) or isinstance(value, list):
        return len(value)
    return value


def process_record(record: dict, counts: Optional[List[str]]) -> Tuple[str, dict]:
    """
    processed an MDS record from the search
    returns the id and record, if an entry in counts is found in the record the length is returned
    instead of the entry.
    """
    _id = record["_id"]
    normalized = record["_source"]
    for c in counts:
        if c in normalized:
            normalized[c] = count(normalized[c])
    return _id, normalized


async def get_all_metadata(limit, offset, counts: Optional[str] = None, flatten=False):
    """
    Queries elastic search for metadata and returns up to the limit
    offset: starting index to return
    counts: converts the count of the entry[count] if it is a dict or array
    returns:

    flattened == true
    results : MDS results as a dict
              paging info

    flattened == false
    results : {
        commonsA: metadata
        commonsB: metadata
        ...
        },
        paging info

    The counts parameter provides a way to "compress" an array field to it's length.
    For example:
        if the record is:
                {"count": [1, 2, 3, 4], "name": "my_name"}
        then setting counts=count the result would be:
            {"count": 4, "name": "my_name"}

    counts can take a comma separated list of field names:
            {
            "count": [1, 2, 3, 4],
            "__manifest" : [
                { "filename": "filename1.txt", "filesize": 1000 },
                { "filename": "filename2.txt", "filesize": 5555 },
            ],
            "name": "my_name"
            }

       setting counts=count,__manifest the result would be:
            {
            "count": 4,
            "__manifest" : 2,
            "name": "my_name"
            }

        if a counts field is not a list or dict then it is unchanged, unless it
        is null, in which case the field will be set to 0
    """
    try:
        res = elastic_search_client.search(
            index=AGG_MDS_INDEX,
            body={"size": limit, "from": offset, "query": {"match_all": {}}},
        )
        hitsTotal = res["hits"]["total"]
        toReduce = counts.split(",") if counts is not None else None
        if flatten:
            flat = []
            for record in res["hits"]["hits"]:
                rid, normalized = process_record(record, toReduce)
                flat.append({rid: {"gen3_discovery": normalized}})
            return {
                "results": flat,
                "pagination": {
                    "hits": hitsTotal,
                    "offset": offset,
                    "pageSize": limit,
                    "pages": ceil(int(hitsTotal) / limit),
                },
            }
        else:
            byCommons = {
                "results": {},
                "pagination": {
                    "hits": hitsTotal,
                    "offset": offset,
                    "pageSize": limit,
                    "pages": ceil(int(hitsTotal) / limit),
                },
            }
            for record in res["hits"]["hits"]:
                rid, normalized = process_record(record, toReduce)
                commons_name = normalized["commons_name"]
                if commons_name not in byCommons["results"]:
                    byCommons["results"][commons_name] = []
                byCommons["results"][commons_name].append(
                    {rid: {"gen3_discovery": normalized}}
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


async def get_commons_attribute(name):
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
