import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Union

import jsonpath_ng as jp
from dataclasses_json import dataclass_json
from elasticsearch import Elasticsearch

from mds.config import (
    AGG_MDS_NAMESPACE,
    ES_RETRY_LIMIT,
    ES_RETRY_INTERVAL,
)

AGG_MDS_INDEX = f"{AGG_MDS_NAMESPACE}-commons-index"

nestedFields = {}


@dataclass_json
@dataclass
class FacetValue:
    """
    A facet is a field that is used to group data, and the number of times each value appears
    """

    operator: str
    values: field(default_factory=list)


@dataclass_json
@dataclass
class FacetSearchParams:
    rootPath: str
    keyField: str
    valueField: str
    facets: Dict[str, FacetValue] = field(default_factory=dict)
    limit: int = 10
    offset: int = 0
    operator: str = "AND"


# given an array of strings separated by '.', build a nested dictionary for each string separated by '.'
# example: ['a.b.c', 'a.b.d', 'a.b.e'] -> {'a': {'b': {'c': {}, 'd': {}, 'e': {}}}}
def build_nested_field_dictionary(paths: List[str]) -> Dict[str, Any]:
    global nestedFields
    nested_dict = {}
    for path in paths:
        parts = path.split(".")
        current_dict = nested_dict
        for i in range(0, len(parts)):
            if parts[i] not in current_dict:
                current_dict[parts[i]] = {}
            current_dict = current_dict[parts[i]]
    nestedFields = nested_dict
    return nested_dict


# given a nested dictionary, and a string value whos prefix is a nested path return the matching path from the nested dictionary
# example: {'a': {'b': {'c': {}, 'd': {}, 'e': {}}}} and 'a.b.x' -> "a.b"
def find_nested_path(value: str) -> str:
    global nestedFields
    parts = value.split(".")
    current_dict = nestedFields
    for i in range(0, len(parts)):
        if parts[i] not in current_dict:
            return ".".join(parts[0:i]) + "."
        current_dict = current_dict[parts[i]]
    return value


# given a field and a value, return the operator that should be used to query the field
def classify_query_operator(field_name: Union[str, List[str]], value: str):
    if isinstance(field_name, list):
        return "multi_match"
    if isinstance(value, list):
        return "match_phrase"
    if "*" in value:
        return "wildcard"
    if "/" in value:
        return "regexp"
    return "match"


def query_by_operator(
    field_name: Union[str, List[str]], value: Any, function: str
) -> Any:
    if function == "match":
        return {"match": {field_name: value}}
    elif function == "match_phrase":
        return {"match_phrase": {field_name: value}}
    elif function == "multi_match":
        return {"multi_match": {"query": value, "fields": field_name}}
    elif function == "wildcard":
        return {"wildcard": {field_name: value}}
    elif function == "regexp":
        return {"regexp": {field_name: value}}
    elif function == "exists":
        return {"exists": {"field": field_name}}
    elif function == "useValue":
        return value
    elif function == "counts":
        return {field_name.replace(".", "__"): {"value_count": {"field": field_name}}}
    elif function == "sum":
        return {field_name.replace(".", "__"): {"sum": {"field": field_name}}}
    elif function == "terms":
        return {field_name.replace(".", "__"): {"terms": {"field": field_name}}}
    else:
        return {"match": {field_name: value}}


def build_nested_search_query(nested_path, full_path, value, level=0, function="match"):
    if level == len(nested_path.split(".")) - 1:
        # final leaf where search is performed
        return query_by_operator(full_path, value, function)

    parts = nested_path.split(".")
    field = [parts[x] for x in range(0, level + 1)]
    field = ".".join(field)
    return {
        "nested": {
            "path": field,
            "query": build_nested_search_query(
                nested_path, full_path, value, level + 1, function
            ),
        }
    }


def build_nested_agg_query(nested_path, full_path, value, level=0, function="match"):
    if level == len(nested_path.split(".")) - 1:
        # final leaf where search is performed
        return query_by_operator(full_path, value, function)

    parts = nested_path.split(".")
    field = [parts[x] for x in range(0, level + 1)]
    field = ".".join(field)
    return {
        field: {
            "nested": {
                "path": field,
            },
            "aggs": build_nested_agg_query(
                nested_path, full_path, value, level + 1, function
            ),
        }
    }


def build_nested_exists_count_query(nested_path, full_path, level=0):
    if level >= len(nested_path.split(".")) - 1:
        return {"exists": {"field": full_path}}

    parts = nested_path.split(".")
    field = [parts[x] for x in range(0, level + 1)]
    field = ".".join(field)
    return {
        "nested": {
            "path": field,
            "query": build_nested_exists_count_query(nested_path, full_path, level + 1),
        }
    }


def build_search_query(path, value, limit=10, offset=0, function="match"):
    nestedPath = find_nested_path(path)
    if len(nestedPath) == 0:
        return None
    return {
        "size": limit,
        "from": offset,
        "query": build_nested_search_query(nestedPath, path, value, function=function),
    }


LogicalOperatorMap = {"AND": "must", "OR": "should", "NOT": "must_not"}


def build_multi_search_query(
    paths: List[str], value, limit=10, offset=0, op="OR", operator="match"
):
    queries = [
        build_search_query(path, value, limit, offset, operator) for path in paths
    ]
    return {
        "size": limit,
        "from": offset,
        "query": {
            "bool": {
                LogicalOperatorMap[op]: [
                    query["query"] for query in queries if query is not None
                ]
            }
        },
    }


def build_aggregation_query(path, function="counts"):
    nested_path = find_nested_path(
        path,
    )
    if len(nested_path) == 0:
        return None
    return {
        "size": 0,
        "aggs": build_nested_agg_query(nested_path, path, None, 0, function),
    }


# given a facet and a list of values, return a query that will return the number of documents that have the facet values
def build_key_value_query(keyPath, valuePath, facet, facet_value: FacetValue):
    return {
        "bool": {
            LogicalOperatorMap[facet_value["operator"]]: [
                {"match": {keyPath: facet}},
                {"terms": {valuePath: facet_value["values"]}},
            ]
        }
    }


def build_nested_faceted_search_query(
    root_path,
    facets_and_values,
    limit=10,
    offset=0,
    op="OR",
    key_field="key",
    value_field="value",
):
    keyQueries = []
    for facet, values in facets_and_values.items():
        keyQueries.append(
            build_key_value_query(
                f"{root_path}.{key_field}", f"{root_path}.{value_field}", facet, values
            )
        )

    query = {"bool": {LogicalOperatorMap[op]: keyQueries}}

    return build_search_query(f"{root_path}.", query, limit, offset, "useValue")


def build_facet_search_query(queryParams):
    return build_nested_faceted_search_query(
        queryParams["rootPath"],
        queryParams["facets"],
        queryParams["limit"],
        queryParams["offset"],
        queryParams["operator"],
        queryParams["keyField"],
        queryParams["valueField"],
    )


def build_exist_count_query(path):
    nestedPath = find_nested_path(path)
    if len(nestedPath) == 0:
        return {}
    return {"query": build_nested_exists_count_query(nestedPath, path)}


def main():
    init()
    # TODO: make this a unit test
    nestedPath = find_nested_path("gen3_discovery._hdp_uid")
    query = {
        "size": 10,
        "from": 0,
        "query": build_nested_exists_count_query(
            nestedPath + ".", "gen3_discovery._hdp_uid", 0
        ),
    }
    print("query:", json.dumps(query, indent=2))

    nestedPath = find_nested_path(
        "gen3_discovery.study_metadata.citation.investigators.investigator_last_name"
    )
    query = {
        "size": 10,
        "from": 0,
        "query": build_nested_search_query(
            nestedPath + ".",
            "gen3_discovery.study_metadata.citation.investigators.investigator_last_name",
            "Ruparel",
        ),
    }
    print("query:", json.dumps(query, indent=2))

    # Sample FacetSearchPayload
    facetsAndValues = {
        "rootPath": "gen3_discovery.advSearchFilters",
        "keyField": "key",
        "valueField": "value",
        "limit": 10,
        "offset": 0,
        "operator": "OR",
        "facets": {
            "Age": {
                "operator": "AND",
                "values": [
                    "Adult (19 to 44 years)",
                    "Middle aged adult (45 to 64 years)",
                ],
            },
            "Subject Type": {"operator": "OR", "values": ["Human"]},
        },
    }

    query = build_facet_search_query(facetsAndValues)
    print("query:", json.dumps(query, indent=2))

    query = build_aggregation_query("gen3_discovery._hdp_uid")
    print("query:", json.dumps(query, indent=2))

    query = build_aggregation_query("gen3_discovery.doi_publication_year", "terms")
    print("query:", json.dumps(query, indent=2))


def init_search_fields_from_mapping():
    raw_data = elastic_search_client.indices.get_mapping(index=AGG_MDS_INDEX)

    # get paths to 'nested' fields using jsonpath_ng
    nested = [
        str(match.full_path).replace("properties.", "").replace(".type", "")
        for match in jp.parse("$..type").find(raw_data[AGG_MDS_INDEX]["mappings"])
        if match.value == "nested"
    ]
    # TODO build a list of all fields in the index
    # allFields = [str(match.full_path).replace('.keyword','').replace('properties.','').replace('.type','').replace('.fields','') for match in jp.parse('$..type').find(raw_data[AGG_MDS_INDEX]["mappings"]['commons'])]

    # set the global variable nestedFields to a nested dictionary of all the nested fields
    build_nested_field_dictionary(nested)


def init(hostname: str = "0.0.0.0", port: int = 9200):
    global elastic_search_client
    elastic_search_client = Elasticsearch(
        [hostname],
        scheme="http",
        port=port,
        timeout=ES_RETRY_INTERVAL,
        max_retries=ES_RETRY_LIMIT,
        retry_on_timeout=True,
    )
    init_search_fields_from_mapping()


if __name__ == "__main__":
    main()
