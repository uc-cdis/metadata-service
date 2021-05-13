import json
import os
from pathlib import Path

from tempfile import NamedTemporaryFile
from mds.agg_mds.commons import (
    parse_config,
    parse_config_from_file,
    Commons,
    MDSInstance,
)


def test_parse_config():
    assert parse_config(
        {
            "commons": {
                "mds_url": "http://mds",
                "commons_url": "http://commons",
                "fields_to_columns": {
                    "short_name": "name",
                    "full_name": "full_name",
                    "_subjects_count": "_subjects_count",
                    "study_id": "study_id",
                    "_unique_id": "_unique_id",
                    "study_description": "study_description",
                },
            }
        }
    ) == Commons(
        {
            "commons": MDSInstance(
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
            )
        }
    )


def test_parse_config_from_file():
    with NamedTemporaryFile(mode="w+", delete=False) as fp:
        json.dump(
            {
                "mycommons": {
                    "mds_url": "http://mds",
                    "commons_url": "http://commons",
                    "fields_to_columns": {
                        "short_name": "name",
                        "full_name": "full_name",
                        "_subjects_count": "_subjects_count",
                        "study_id": "study_id",
                        "_unique_id": "_unique_id",
                        "study_description": "study_description",
                    },
                },
            },
            fp,
        )
    config = parse_config_from_file(Path(fp.name))
    assert (
        config.to_json()
        == Commons(
            {
                "mycommons": MDSInstance(
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
                )
            }
        ).to_json()
    )
