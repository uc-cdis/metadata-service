from more_itertools import side_effect
import respx
import asyncio
from mds.agg_mds.adapters import (
    get_metadata,
    get_json_path_value,
    strip_email,
    strip_html,
    normalize_value,
    normalize_tags,
    add_icpsr_source_url,
    FieldFilters,
    get_json_path_value,
    add_clinical_trials_source_url,
    uppercase,
    rename_double_underscore_keys,
)
import httpx


def test_filters_with_bad_entries():
    assert strip_email(100) == 100
    assert strip_html(99) == 99
    assert add_icpsr_source_url(77) == 77


def test_non_existing_filters():
    assert FieldFilters().execute("nofilter", "passthru") == "passthru"


def test_json_path():
    assert get_json_path_value(None, {}) is None
    assert get_json_path_value("shark", {"shark": ["great", "white"]}) == [
        "great",
        "white",
    ]


@respx.mock
def test_drs_indexd():
    json_data = [
        {
            "hints": [".*dg\\.XXTS.*"],
            "host": "https://mytest1.commons.io/",
            "name": "DataSTAGE",
            "type": "indexd",
        },
        {
            "hints": [".*dg\\.TSXX.*"],
            "host": "https://commons2.io/index/",
            "name": "Environmental DC",
            "type": "indexd",
        },
    ]

    expected = {
        "info": {"created": "07/07/2022 15:28:46:UTC"},
        "cache": {
            "dg.XXTS": {
                "host": "mytest1.commons.io",
                "name": "DataSTAGE",
                "type": "indexd",
            },
            "dg.TSXX": {
                "host": "commons2.io",
                "name": "Environmental DC",
                "type": "indexd",
            },
        },
    }

    respx.get("http://test/index/_dist").mock(
        return_value=httpx.Response(
            status_code=200,
            json=json_data,
        )
    )

    results = get_metadata(
        "drs_indexd",
        "http://test",
        filters=None,
    )

    assert results["cache"] == expected["cache"]

    respx.get("http://test/index/_dist").mock(
        return_value=httpx.Response(
            status_code=404,
            json=None,
        )
    )

    results = get_metadata(
        "drs_indexd",
        "http://test",
        filters=None,
    )

    assert results == {"results": {}}


def test_missing_adapter():
    try:
        get_metadata("notAKnownAdapter", "http://test/ok/", None, None)
    except Exception as err:
        assert isinstance(err, ValueError) == True


def test_json_path_expression():
    sample1 = {
        "study1": {
            "study_description_summary": "This is a summary",
            "id": "2334.5.555ad",
            "contributors": ["Bilbo Baggins"],
            "datasets": ["results1.csv", "results2.csv", "results3.csv"],
        },
        "study3": {
            "study_description_summary": "This is another summary",
            "id": "333.33222.ad",
            "datasets": ["results4.csv", "results5.csv", "results6.csv"],
        },
    }

    assert (
        get_json_path_value("study1.study_description_summary", sample1)
        == "This is a summary"
    )

    # test non existent path
    assert get_json_path_value("study2.study_description_summary", sample1) is None

    # test bad path
    assert get_json_path_value(".contributors", sample1) is None

    # test single array
    assert get_json_path_value("study1.contributors", sample1) == ["Bilbo Baggins"]

    # test array whose length is greater than 1
    assert get_json_path_value("*.datasets", sample1) == [
        ["results1.csv", "results2.csv", "results3.csv"],
        ["results4.csv", "results5.csv", "results6.csv"],
    ]


def test_normalize_value_with_no_mapping():
    """
    Test that the function returns the input value when mapping is None.
    """
    value = "test_value"
    result = normalize_value(value)
    assert result == "test_value"


def test_normalize_value_with_non_matching_mapping():
    """
    Test that the function returns the input value when it is not found in the mapping.
    """
    value = "test_value"
    mapping = {"other_value": "mapped_value"}
    result = normalize_value(value, mapping)
    assert result == "test_value"


def test_normalize_value_with_matching_mapping():
    """
    Test that the function returns the mapped value when the input value is found in the mapping.
    """
    value = "test_value"
    mapping = {"test_value": "mapped_value"}
    result = normalize_value(value, mapping)
    assert result == "mapped_value"


def test_normalize_value_with_non_string_input():
    """
    Test that the function returns the input value unchanged when the input is not a string.
    """
    value = 12345
    mapping = {"12345": "mapped_value"}
    result = normalize_value(value, mapping)
    assert result == 12345


def test_normalize_value_with_empty_mapping():
    """
    Test that the function returns the input value when the mapping is empty.
    """
    value = "test_value"
    mapping = {}
    result = normalize_value(value, mapping)
    assert result == "test_value"


def test_normalize_value_with_none_as_value():
    """
    Test that the function returns None when the input value is None.
    """
    value = None
    mapping = {"test_value": "mapped_value"}
    result = normalize_value(value, mapping)
    assert result is None


def test_normalize_tags_with_no_mapping():
    """
    Test that the function returns the original tags unmodified
    when no mapping is provided.
    """
    input_tags = [{"name": "tag1", "category": "cat1"}]
    result = normalize_tags(input_tags)
    assert result == input_tags


def test_normalize_tags_with_empty_mapping():
    """
    Test that the function returns the original tags unmodified
    when an empty mapping is provided.
    """
    input_tags = [{"name": "tag1", "category": "cat1"}]
    result = normalize_tags(input_tags, mapping={})
    assert result == input_tags


def test_normalize_tags_with_matching_mapping():
    """
    Test that the function updates the 'name' field correctly
    based on the provided mapping.
    """
    input_tags = [{"name": "tag1", "category": "cat1"}]
    mapping = {"cat1": {"tag1": "tag1_updated"}}
    expected = [{"name": "tag1_updated", "category": "cat1"}]
    result = normalize_tags(input_tags, mapping=mapping)
    assert result == expected


def test_normalize_tags_with_non_matching_category():
    """
    Test that the function leaves the tags unchanged
    when the category is not in the mapping.
    """
    input_tags = [{"name": "tag1", "category": "cat1"}]
    mapping = {"cat2": {"tag1": "tag1_updated"}}
    result = normalize_tags(input_tags, mapping=mapping)
    assert result == input_tags


def test_normalize_tags_with_non_matching_name():
    """
    Test that the function leaves the tags unchanged
    when the name is not in the mapping for the given category.
    """
    input_tags = [{"name": "tag1", "category": "cat1"}]
    mapping = {"cat1": {"tag2": "tag2_updated"}}
    result = normalize_tags(input_tags, mapping=mapping)
    assert result == input_tags


def test_normalize_tags_with_missing_name_key():
    """
    Test that the function leaves tags without a 'name' key unchanged.
    """
    input_tags = [{"category": "cat1"}]
    mapping = {"cat1": {"tag1": "tag1_updated"}}
    result = normalize_tags(input_tags, mapping=mapping)
    assert result == input_tags


def test_normalize_tags_with_missing_category_key():
    """
    Test that the function leaves tags without a 'category' key unchanged.
    """
    input_tags = [{"name": "tag1"}]
    mapping = {"cat1": {"tag1": "tag1_updated"}}
    result = normalize_tags(input_tags, mapping=mapping)
    assert result == input_tags


def test_normalize_tags_with_partial_mapping_applied():
    """
    Test that the function updates the 'name' field for tags with matches
    in the mapping and leaves others unchanged.
    """
    input_tags = [
        {"name": "tag1", "category": "cat1"},
        {"name": "tag2", "category": "cat2"},
    ]
    mapping = {"cat1": {"tag1": "tag1_updated"}}
    expected = [
        {"name": "tag1_updated", "category": "cat1"},
        {"name": "tag2", "category": "cat2"},
    ]
    result = normalize_tags(input_tags, mapping=mapping)
    assert result == expected


def test_normalize_tags_empty_input_tags():
    """
    Test that the function returns an empty list when given
    an empty input list.
    """
    result = normalize_tags([], mapping={"cat1": {"tag1": "tag1_updated"}})
    assert result == []


def test_add_clinical_trials_source_url():
    integer = 1
    assert add_clinical_trials_source_url(integer) == 1


def test_uppercase():
    interger = 1
    assert uppercase(interger) == 1


def test_single_dict_with_double_underscore_keys(self):
    """Test renaming double underscore keys in a single dictionary."""
    input_dict = {"__private": "secret", "__name": "test", "normal": "value"}
    expected = {"_private": "secret", "_name": "test", "normal": "value"}
    result = rename_double_underscore_keys(input_dict)
    self.assertEqual(result, expected)


def test_single_dict_no_double_underscore_keys(self):
    """Test dictionary with no double underscore keys remains unchanged."""
    input_dict = {"normal": "value", "_single": "underscore", "no_underscore": "test"}
    expected = {"normal": "value", "_single": "underscore", "no_underscore": "test"}
    result = rename_double_underscore_keys(input_dict)
    self.assertEqual(result, expected)


def test_empty_dict(self):
    """Test empty dictionary."""
    input_dict = {}
    expected = {}
    result = rename_double_underscore_keys(input_dict)
    self.assertEqual(result, expected)


def test_dict_with_non_string_keys(self):
    """Test dictionary with non-string keys."""
    input_dict = {123: "number", ("a", "b"): "tuple", "__string": "value"}
    expected = {123: "number", ("a", "b"): "tuple", "_string": "value"}
    result = rename_double_underscore_keys(input_dict)
    self.assertEqual(result, expected)


def test_dict_with_only_double_underscores(self):
    """Test dictionary with key that is exactly '__'."""
    input_dict = {"__": "double_underscore_only"}
    expected = {"_": "double_underscore_only"}
    result = rename_double_underscore_keys(input_dict)
    self.assertEqual(result, expected)


def test_list_of_dicts(self):
    """Test list containing multiple dictionaries."""
    input_list = [
        {"__id": 1, "__type": "user", "name": "Alice"},
        {"__id": 2, "__type": "admin", "name": "Bob"},
        {"normal_key": "no change needed"},
    ]
    expected = [
        {"_id": 1, "_type": "user", "name": "Alice"},
        {"_id": 2, "_type": "admin", "name": "Bob"},
        {"normal_key": "no change needed"},
    ]
    result = rename_double_underscore_keys(input_list)
    self.assertEqual(result, expected)


def test_empty_list(self):
    """Test empty list."""
    input_list = []
    expected = []
    result = rename_double_underscore_keys(input_list)
    self.assertEqual(result, expected)


def test_mixed_list(self):
    """Test list with dictionaries and other types."""
    input_list = [{"__test": "value"}, "string_item", 123, {"__another": "dict"}, None]
    expected = [{"_test": "value"}, "string_item", 123, {"_another": "dict"}, None]
    result = rename_double_underscore_keys(input_list)
    self.assertEqual(result, expected)


def test_list_with_empty_dicts(self):
    """Test list containing empty dictionaries."""
    input_list = [{}, {"__key": "value"}, {}]
    expected = [{}, {"_key": "value"}, {}]
    result = rename_double_underscore_keys(input_list)
    self.assertEqual(result, expected)


def test_non_dict_non_list_input(self):
    """Test input that is neither dict nor list."""
    inputs = ["string", 123, None, ("tuple",), {"set"}]
    for input_val in inputs:
        with self.subTest(input_val=input_val):
            result = rename_double_underscore_keys(input_val)
            self.assertEqual(result, input_val)


def test_original_not_modified(self):
    """Test that original input is not modified (immutability)."""
    original_dict = {"__private": "secret", "normal": "value"}
    original_copy = original_dict.copy()

    rename_double_underscore_keys(original_dict)
    self.assertEqual(original_dict, original_copy)

    original_list = [{"__id": 1}, {"normal": "value"}]
    original_list_copy = [d.copy() for d in original_list]

    rename_double_underscore_keys(original_list)
    self.assertEqual(original_list, original_list_copy)


def test_keys_starting_with_triple_underscore(self):
    """Test keys that start with more than two underscores."""
    input_dict = {"___private": "secret", "____name": "test"}
    expected = {"__private": "secret", "___name": "test"}
    result = rename_double_underscore_keys(input_dict)
    self.assertEqual(result, expected)


def test_keys_with_underscore_not_at_start(self):
    """Test keys with underscores not at the beginning."""
    input_dict = {"my__private": "secret", "test__name": "value"}
    expected = {"my__private": "secret", "test__name": "value"}
    result = rename_double_underscore_keys(input_dict)
    self.assertEqual(result, expected)
