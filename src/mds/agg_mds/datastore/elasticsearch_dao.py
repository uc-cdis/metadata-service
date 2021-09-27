from elasticsearch import Elasticsearch, exceptions as es_exceptions
from typing import List, Dict
import json
from mds import logger
from mds.config import AGG_MDS_NAMESPACE


# TODO WFH Why do we have both __manifest and _file_manifest?
# TODO WFH These are bugs. If we have to check whether an object is a string or
# an object, the data is bad.
FIELD_NORMALIZERS = {
    "__manifest": "object",
    "_file_manifest": "object",
    "advSearchFilters": "object",
    "data_dictionary": "object",
    "sites": "number",
}


AGG_MDS_INDEX = f"{AGG_MDS_NAMESPACE}-commons-index"
AGG_MDS_TYPE = "commons"


AGG_MDS_INFO_INDEX = f"{AGG_MDS_NAMESPACE}-commons-info-index"
AGG_MDS_INFO_TYPE = "commons-info"


MAPPING = {
    "mappings": {
        "commons": {
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
}

elastic_search_client = None


async def init(hostname: str = "0.0.0.0", port: int = 9200):
    global elastic_search_client
    elastic_search_client = Elasticsearch(
        [hostname],
        scheme="http",
        port=port,
        timeout=30,
        max_retries=7,
        retry_on_timeout=True,
    )


async def drop_all():
    for index in [AGG_MDS_INDEX, AGG_MDS_INFO_INDEX]:
        res = elastic_search_client.indices.delete(index=index, ignore=[400, 404])
        logger.debug(f"deleted index: {index}")

    try:
        res = elastic_search_client.indices.create(index=AGG_MDS_INDEX, body=MAPPING)
        logger.debug(f"created index {AGG_MDS_INDEX}: {res}")
    except es_exceptions.RequestError as ex:
        if ex.error == "resource_already_exists_exception":
            logger.warning(f"index already exists: {AGG_MDS_INDEX}")
            pass  # Index already exists. Ignore.
        else:  # Other exception - raise it
            raise ex

    try:
        res = elastic_search_client.indices.create(
            index=AGG_MDS_INFO_INDEX,
        )
        logger.debug(f"created index {AGG_MDS_INFO_INDEX}: {res}")

    except es_exceptions.RequestError as ex:
        if ex.error == "resource_already_exists_exception":
            logger.warning(f"index already exists: {AGG_MDS_INFO_INDEX}")
            pass  # Index already exists. Ignore.
        else:  # Other exception - raise it
            raise ex


def normalize_field(doc, key, normalize_type):
    try:
        if normalize_type == "object" and isinstance(doc[key], str):
            value = doc[key]
            doc[key] = None if value is "" else json.loads(value)
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

        for field in FIELD_NORMALIZERS.keys():
            if field in doc:
                normalize_field(doc, field, FIELD_NORMALIZERS[field])

        elastic_search_client.index(
            index=AGG_MDS_INDEX, doc_type=AGG_MDS_TYPE, id=key, body=doc
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


async def get_all_metadata(limit, offset):
    try:
        res = elastic_search_client.search(
            index=AGG_MDS_INDEX,
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
        res = elastic_search_client.search(
            index=AGG_MDS_INDEX,
            body={"query": {"match": {"commons_name.keyword": name}}},
        )
        return [x["_source"] for x in res["hits"]["hits"]]
    except Exception as error:
        logger.error(error)
        return {}


async def metadata_tags(name):
    try:
        return elastic_search_client.search(
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
