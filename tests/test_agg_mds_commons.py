from mds.agg_mds.commons import (
    parse_config,
    Commons,
    Config,
    Settings,
    ColumnsToFields,
    FieldDefinition,
    MDSInstance,
    AdapterMDSInstance,
)


def test_convert_tp_schema():
    schema = FieldDefinition(
        type="object",
        properties={
            "_subjects_count": FieldDefinition(type="integer"),
            "year_awarded": FieldDefinition(type="integer", default=2000),
            "__manifest": FieldDefinition(
                type="array",
                properties={
                    "file_name": FieldDefinition(type="string"),
                    "file_size": FieldDefinition(type="integer"),
                },
            ),
            "tags": FieldDefinition(type="array"),
            "study_description": FieldDefinition(type="string"),
            "short_name": FieldDefinition(type="string"),
            "full_name": FieldDefinition(type="string"),
            "_unique_id": FieldDefinition(type="string"),
            "study_id": FieldDefinition(type="string"),
            "study_url": FieldDefinition(type="string"),
            "commons_url": FieldDefinition(type="string"),
            "authz": FieldDefinition(type="string"),
        },
    )

    converted = schema.to_schema(True)

    expected = {
        "properties": {
            "__manifest": {
                "properties": {
                    "file_name": {
                        "fields": {
                            "analyzed": {
                                "analyzer": "ngram_analyzer",
                                "search_analyzer": "search_analyzer",
                                "term_vector": "with_positions_offsets",
                                "type": "text",
                            }
                        },
                        "type": "keyword",
                    },
                    "file_size": {"type": "long"},
                },
                "type": "nested",
            },
            "_subjects_count": {"type": "long"},
            "_unique_id": {
                "fields": {
                    "analyzed": {
                        "analyzer": "ngram_analyzer",
                        "search_analyzer": "search_analyzer",
                        "term_vector": "with_positions_offsets",
                        "type": "text",
                    }
                },
                "type": "keyword",
            },
            "authz": {
                "fields": {
                    "analyzed": {
                        "analyzer": "ngram_analyzer",
                        "search_analyzer": "search_analyzer",
                        "term_vector": "with_positions_offsets",
                        "type": "text",
                    }
                },
                "type": "keyword",
            },
            "commons_url": {
                "fields": {
                    "analyzed": {
                        "analyzer": "ngram_analyzer",
                        "search_analyzer": "search_analyzer",
                        "term_vector": "with_positions_offsets",
                        "type": "text",
                    }
                },
                "type": "keyword",
            },
            "full_name": {
                "fields": {
                    "analyzed": {
                        "analyzer": "ngram_analyzer",
                        "search_analyzer": "search_analyzer",
                        "term_vector": "with_positions_offsets",
                        "type": "text",
                    }
                },
                "type": "keyword",
            },
            "short_name": {
                "fields": {
                    "analyzed": {
                        "analyzer": "ngram_analyzer",
                        "search_analyzer": "search_analyzer",
                        "term_vector": "with_positions_offsets",
                        "type": "text",
                    }
                },
                "type": "keyword",
            },
            "study_description": {
                "fields": {
                    "analyzed": {
                        "analyzer": "ngram_analyzer",
                        "search_analyzer": "search_analyzer",
                        "term_vector": "with_positions_offsets",
                        "type": "text",
                    }
                },
                "type": "keyword",
            },
            "study_id": {
                "fields": {
                    "analyzed": {
                        "analyzer": "ngram_analyzer",
                        "search_analyzer": "search_analyzer",
                        "term_vector": "with_positions_offsets",
                        "type": "text",
                    }
                },
                "type": "keyword",
            },
            "study_url": {
                "fields": {
                    "analyzed": {
                        "analyzer": "ngram_analyzer",
                        "search_analyzer": "search_analyzer",
                        "term_vector": "with_positions_offsets",
                        "type": "text",
                    }
                },
                "type": "keyword",
            },
            "tags": {"type": "nested"},
            "year_awarded": {
                "type": "long",
            },
        },
        "type": "nested",
    }
    assert converted == expected

    converted = schema.to_schema(False, True)

    expected = {
        "type": "object",
        "properties": {
            "_subjects_count": {"type": "integer", "description": ""},
            "year_awarded": {"type": "integer", "description": "", "default": 2000},
            "__manifest": {
                "type": "array",
                "properties": {
                    "file_name": {"type": "string", "description": ""},
                    "file_size": {"type": "integer", "description": ""},
                },
                "description": "",
            },
            "tags": {"type": "array", "description": ""},
            "study_description": {"type": "string", "description": ""},
            "short_name": {"type": "string", "description": ""},
            "full_name": {"type": "string", "description": ""},
            "_unique_id": {"type": "string", "description": ""},
            "study_id": {"type": "string", "description": ""},
            "study_url": {"type": "string", "description": ""},
            "commons_url": {"type": "string", "description": ""},
            "authz": {"type": "string", "description": ""},
        },
        "description": "",
    }

    assert converted == expected


def test_parse_config():
    results = parse_config(
        """
        {
            "configuration": {
                "schema": {
                    "_subjects_count": {
                        "type": "integer"
                    },
                    "year_awarded": {
                        "type": "integer"
                    },
                    "__manifest": {
                        "type": "array",
                        "properties": {
                            "file_name": {
                                "type": "string"
                            },
                            "file_size": {
                                "type": "integer"
                            }
                        }
                    },
                    "tags": {
                        "type": "array"
                    },
                    "study_description": {},
                    "short_name": {},
                    "full_name": {},
                    "_unique_id": {},
                    "study_id": {},
                    "study_url": {},
                    "commons_url": {},
                    "authz": {
                        "type": "string"
                    }
                }
            },
            "gen3_commons": {
                "my_gen3_commons": {
                    "mds_url": "http://mds",
                    "commons_url": "http://commons",
                    "columns_to_fields": {
                        "short_name": "name",
                        "full_name": "full_name",
                        "_subjects_count": "_subjects_count",
                        "study_id": "study_id",
                        "_unique_id": "_unique_id",
                        "study_description": "study_description"
                    }
                }
            },
            "adapter_commons": {
                "non_gen3_commons": {
                    "mds_url": "http://non-gen3",
                    "commons_url": "non-gen3",
                    "adapter": "icpsr"
                },
                "another_gen3_commons": {
                    "mds_url": "http://another-gen3",
                    "commons_url": "another-gen3",
                    "adapter": "gen3",
                    "study_data_field" : "my_metadata",
                    "data_dict_field" : "my_data_dict"
                }
            }
        }
        """
    )
    expected = Commons(
        configuration=Config(
            settings=Settings(),
            schema={
                "_subjects_count": FieldDefinition(type="integer"),
                "year_awarded": FieldDefinition(type="integer"),
                "__manifest": FieldDefinition(
                    type="array",
                    properties={
                        "file_name": FieldDefinition(type="string"),
                        "file_size": FieldDefinition(type="integer"),
                    },
                ),
                "tags": FieldDefinition(type="array"),
                "study_description": FieldDefinition(type="string"),
                "short_name": FieldDefinition(type="string"),
                "full_name": FieldDefinition(type="string"),
                "_unique_id": FieldDefinition(type="string"),
                "study_id": FieldDefinition(type="string"),
                "study_url": FieldDefinition(type="string"),
                "commons_url": FieldDefinition(type="string"),
                "authz": FieldDefinition(type="string"),
            },
        ),
        gen3_commons={
            "my_gen3_commons": MDSInstance(
                "http://mds",
                "http://commons",
                {
                    "short_name": "name",
                    "full_name": "full_name",
                    "_subjects_count": "_subjects_count",
                    "study_id": "study_id",
                    "_unique_id": "_unique_id",
                    "study_description": "study_description",
                },
            ),
        },
        adapter_commons={
            "non_gen3_commons": AdapterMDSInstance(
                "http://non-gen3",
                "non-gen3",
                "icpsr",
            ),
            "another_gen3_commons": AdapterMDSInstance(
                "http://another-gen3",
                "another-gen3",
                "gen3",
                study_data_field="my_metadata",
                data_dict_field="my_data_dict",
            ),
        },
    )

    assert expected == results


def test_normalization():
    val = FieldDefinition(type="integer")
    assert val.normalize_value("100") == 100
    assert val.normalize_value("bear") is None

    val = FieldDefinition(type="number")
    assert val.normalize_value("1.23") == 1.23
    assert val.normalize_value("bear") is None

    val = FieldDefinition(type="array")
    assert val.normalize_value("1.23") == ["1.23"]
    assert val.normalize_value({"foo": "bar"}) == [{"foo": "bar"}]

    val = FieldDefinition(type="array")
    assert val.normalize_value(None) is None

    val = FieldDefinition(type="array")
    assert val.normalize_value("") == []

    val = FieldDefinition(type="object")
    assert val.normalize_value('{"foo" : "bar"}') == {"foo": "bar"}

    val = FieldDefinition(type="object")
    assert val.normalize_value("bear") is None

    val = FieldDefinition(type="string")
    assert val.normalize_value("hello") == "hello"

    val = FieldDefinition(type="bar")
    assert val.normalize_value("hello") == "hello"

    val = FieldDefinition(type="string")
    val.has_default_value() is False

    val = FieldDefinition(type="string", default="hi")
    val.has_default_value() == "hi"

    val = FieldDefinition(type="string")
    assert val.normalize_value(["hello", "how", "are", "you"]) == "hellohowareyou"
    assert val.normalize_value(None) is None


def test_mds_instance():
    val = MDSInstance(
        mds_url="https://test",
        commons_url="http:/commons.io",
    )
    assert val.columns_to_fields is None

    val = MDSInstance(
        mds_url="https://test",
        commons_url="http:/commons.io",
        columns_to_fields={
            "val1": "path:root",
            "value2": {"name": "value1", "default": "bear"},
        },
    )

    assert val.columns_to_fields is not None

    val = ColumnsToFields(name="test", default="bear")
    assert val.get_value({"test": "fox"}) == "fox"
    assert val.get_value({"b": "s"}) == "bear"
