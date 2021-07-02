import json
import os
from pathlib import Path

from mds.agg_mds.commons import (
    parse_config,
    Commons,
    MDSInstance,
    AdapterMDSInstance,
)


def test_parse_config():
    assert parse_config(
        {
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
                        "study_description": "study_description",
                    },
                }
            },
            "adapter_commons": {
                "non_gen3_commons": {
                    "mds_url": "http://non-gen3",
                    "commons_url": "non-gen3",
                    "adapter": "icpsr",
                }
            },
        }
    ) == Commons(
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
