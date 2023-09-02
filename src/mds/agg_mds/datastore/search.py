import json
from typing import Any, Dict, List, Union

nestedFields = {}


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
def classify_query_operator(field: Union[str, List[str]], value: str):
    if isinstance(field, list):
        return "multi_match"
    if isinstance(value, list):
        return "match_phrase"
    if "*" in value:
        return "wildcard"
    if "/" in value:
        return "regexp"
    return "match"


def query_by_operator(field: Union[str, List[str]], value: str, operator: str) -> Any:
    if operator == "match":
        return {"match": {field: value}}
    elif operator == "match_phrase":
        return {"match_phrase": {field: value}}
    elif operator == "multi_match":
        return {"multi_match": {"query": value, "fields": field}}
    elif operator == "wildcard":
        return {"wildcard": {field: value}}
    elif operator == "regexp":
        return {"regexp": {field: value}}
    elif operator == "exists":
        return {"exists": {"field": field}}


def build_nested_search_query(nestedPath, fullPath, value, level=0, operator="match"):
    if level == len(nestedPath.split(".")) - 1:
        # final leaf where search is performed
        return query_by_operator(fullPath, value, operator)

    parts = nestedPath.split(".")
    field = [parts[x] for x in range(0, level + 1)]
    field = ".".join(field)
    return {
        "nested": {
            "path": field,
            "query": build_nested_search_query(
                nestedPath, fullPath, value, level + 1, operator
            ),
        }
    }


def build_nested_exists_count_query(nestedPath, fullPath, level=0):
    if level >= len(nestedPath.split(".")) - 1:
        return {"exists": {"field": fullPath}}

    parts = nestedPath.split(".")
    field = [parts[x] for x in range(0, level + 1)]
    field = ".".join(field)
    return {
        "nested": {
            "path": field,
            "query": build_nested_exists_count_query(nestedPath, fullPath, level + 1),
        }
    }


def build_search_query(path, value, limit=10, offset=0, operator="match"):
    nestedPath = find_nested_path(path)
    if len(nestedPath) == 0:
        return None
    return {
        "size": limit,
        "from": offset,
        "query": build_nested_search_query(nestedPath, path, value, operator=operator),
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


def build_exist_count_query(path):
    nestedPath = find_nested_path(path)
    if len(nestedPath) == 0:
        return {}
    return {"query": build_nested_exists_count_query(nestedPath, path)}


def main():
    # TODO: make this a unit test
    nestedPath = find_nested_path("gen3_discovery._hdp_uid")
    query = {
        "size": 0,
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


if __name__ == "__main__":
    main()
