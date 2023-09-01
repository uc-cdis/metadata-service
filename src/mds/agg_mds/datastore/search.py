import json
from typing import Any, Dict, List

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


def build_nested_search_query(nestedPath, fullPath, value, level=0, operator="match"):
    if level == len(nestedPath.split(".")) - 1:
        return {operator: {fullPath: value}}
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
