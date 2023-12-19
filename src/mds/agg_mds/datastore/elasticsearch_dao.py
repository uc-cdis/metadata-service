from math import ceil
from typing import Any, List, Dict, Union, Optional, Tuple
import jsonpath_ng as jp
from elasticsearch import Elasticsearch, exceptions as es_exceptions

from mds import logger
from mds.agg_mds.datastore.search import (
    build_multi_search_query,
    build_nested_field_dictionary,
    build_search_query,
    build_facet_search_query,
    build_aggregation_query,
)
from mds.config import (
    AGG_MDS_NAMESPACE,
    ES_RETRY_LIMIT,
    ES_RETRY_INTERVAL,
    AGG_MDS_DEFAULT_STUDY_DATA_FIELD,
    AGG_MDS_DEFAULT_DATA_DICT_FIELD,
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


# get the mapping from the elastic search client
def init_search_fields_from_mapping():
    raw_data = elastic_search_client.indices.get_mapping(index=AGG_MDS_INDEX)

    # get paths to 'nested' fields using jsonpath_ng
    nested = [
        str(match.full_path).replace("properties.", "").replace(".type", "")
        for match in jp.parse("$..type").find(
            raw_data[AGG_MDS_INDEX]["mappings"]["commons"]
        )
        if match.value == "nested"
    ]
    # TODO build a list of all fields in the index
    # allFields = [str(match.full_path).replace('.keyword','').replace('properties.','').replace('.type','').replace('.fields','') for match in jp.parse('$..type').find(raw_data[AGG_MDS_INDEX]["mappings"]['commons'])]

    # set the global variable nestedFields to a nested dictionary of all the nested fields
    build_nested_field_dictionary(nested)


async def init(hostname: str = "0.0.0.0", port: int = 9200, support_search=False):
    global elastic_search_client
    elastic_search_client = Elasticsearch(
        [hostname],
        scheme="http",
        port=port,
        timeout=ES_RETRY_INTERVAL,
        max_retries=ES_RETRY_LIMIT,
        retry_on_timeout=True,
    )
    if support_search:
        init_search_fields_from_mapping()


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
        if AGG_MDS_DEFAULT_DATA_DICT_FIELD in d[key]:
            doc[AGG_MDS_DEFAULT_DATA_DICT_FIELD] = d[key][
                AGG_MDS_DEFAULT_DATA_DICT_FIELD
            ]

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
        return {
            "commons": [
                x["key"]
                for x in res["aggregations"][AGG_MDS_DEFAULT_STUDY_DATA_FIELD][
                    "commons_names"
                ]["buckets"]
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
    if AGG_MDS_DEFAULT_STUDY_DATA_FIELD in normalized:
        for c in counts:
            if c in normalized[AGG_MDS_DEFAULT_STUDY_DATA_FIELD]:
                normalized[AGG_MDS_DEFAULT_STUDY_DATA_FIELD][c] = count(
                    normalized[AGG_MDS_DEFAULT_STUDY_DATA_FIELD][c]
                )
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
                flat.append({rid: normalized})
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
                commons_name = normalized[AGG_MDS_DEFAULT_STUDY_DATA_FIELD][
                    "commons_name"
                ]
                if commons_name not in byCommons["results"]:
                    byCommons["results"][commons_name] = []
                byCommons["results"][commons_name].append({rid: normalized})

            return byCommons
    except Exception as error:
        logger.error(error)
        return {}


async def get_all_named_commons_metadata(name="HEAL"):
    try:
        res = elastic_search_client.search(
            index=AGG_MDS_INDEX,
            body={
                "query": {
                    "nested": {
                        "path": AGG_MDS_DEFAULT_STUDY_DATA_FIELD,
                        "query": {
                            "match": {
                                f"{AGG_MDS_DEFAULT_STUDY_DATA_FIELD}.commons_name.keyword": f"{name}"
                            }
                        },
                    }
                }
            },
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


async def search(field: str, term: str, limit=10, offset=0, op="OR"):
    fields = field.split(",")
    if len(fields) > 1:
        query = build_multi_search_query(fields, term, limit, offset, op)
    else:
        query = build_search_query(field, term, limit, offset)

    if query is None:
        return {}
    try:
        data = elastic_search_client.search(
            index=AGG_MDS_INDEX,
            body=query,
        )
        return data["hits"]["hits"]
    except Exception as error:
        logger.error(error)
        return {}


async def facet_search(search_query):
    query = build_facet_search_query(search_query)
    if query is None:
        return {}
    try:
        data = elastic_search_client.search(
            index=AGG_MDS_INDEX,
            body=query,
        )
        return data["hits"]["hits"]
    except Exception as error:
        logger.error(error)
        return {}


async def get_aggs(agg_query, function="count"):
    print("agg_query", agg_query)
    query = build_aggregation_query(agg_query, function)
    print("query", query)
    if query is None:
        return {}
    try:
        data = elastic_search_client.search(
            index=AGG_MDS_INDEX,
            body=query,
        )
        print("data", data)
        query_path = agg_query.replace(".", "__")
        print("query_path", query_path)
        matches = jp.parse(f"aggregations..{query_path}").find(data)
        print("matches", matches)
        return matches[0].value
    except Exception as error:
        logger.error(error)
        return {}


async def get_aggregations(name):
    try:
        res = elastic_search_client.search(
            index=AGG_MDS_INDEX,
            body={
                "size": 0,
                "query": {
                    "constant_score": {
                        "filter": {
                            "match": {
                                f"{AGG_MDS_DEFAULT_STUDY_DATA_FIELD}.commons_name": name
                            },
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
