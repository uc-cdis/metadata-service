import json
import os
from pathlib import Path

from mds.agg_mds.commons import (
    parse_config,
    Commons,
    Config,
    FieldDefinition,
    MDSInstance,
    AdapterMDSInstance,
)


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
                }
            }
        }
        """
    )
    expected = Commons(
        configuration=Config(
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
            }
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
            )
        },
    )

    assert expected == results
