import pytest
from argparse import Namespace
from mds.populate import (
    parse_config_from_file,
    parse_args,
    populate_metadata,
    main,
    filter_entries,
)
from mds.agg_mds.commons import AdapterMDSInstance, MDSInstance, Commons
from mds.agg_mds import adapters
from mds.agg_mds import datastore
import json
from unittest.mock import patch, MagicMock
from conftest import AsyncMock
from tempfile import NamedTemporaryFile
from pathlib import Path


@pytest.mark.asyncio
async def test_parse_args():
    try:
        known_args = parse_args([])
    except BaseException as exception:
        assert exception.code == 2

    known_args = parse_args(["--config", "some/file.json"])
    assert known_args == Namespace(
        config="some/file.json", hostname="localhost", port=6379
    )

    known_args = parse_args(
        ["--config", "some/file.json", "--hostname", "server", "--port", "1000"]
    )
    assert known_args == Namespace(
        config="some/file.json", hostname="server", port=1000
    )


@pytest.mark.asyncio
async def test_populate_metadata():
    with patch.object(datastore, "update_metadata", AsyncMock()) as mock_update:
        await populate_metadata(
            "my_commons",
            MDSInstance(
                mds_url="http://mds",
                commons_url="http://commons",
                columns_to_fields={"column1": "field1"},
            ),
            {"id1": {"gen3_discovery": {"column1": "some data", "tags": {}}}},
        )

        mock_update.assert_called_with(
            "my_commons",
            [
                {
                    "id1": {
                        "gen3_discovery": {
                            "column1": "some data",
                            "tags": {},
                            "field1": "some data",
                            "commons_name": "my_commons",
                        }
                    }
                }
            ],
            ["id1"],
            {},
            {"commons_url": "http://commons"},
        )


@pytest.mark.asyncio
async def test_main():
    with patch("mds.config.USE_AGG_MDS", False):
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            await main(None, "", 0)
        assert pytest_wrapped_e.type == SystemExit
        assert pytest_wrapped_e.value.code == 1

    patch("mds.config.USE_AGG_MDS", True).start()
    patch("mds.populate.pull_mds", MagicMock()).start()
    patch.object(datastore, "init", AsyncMock()).start()
    patch.object(datastore, "drop_all", AsyncMock()).start()
    patch.object(datastore, "get_status", AsyncMock(return_value="OK")).start()
    patch.object(datastore, "close", AsyncMock()).start()
    patch.object(datastore, "update_metadata", AsyncMock()).start()
    patch.object(adapters, "get_metadata", MagicMock()).start()

    await main(
        Commons(
            gen3_commons={
                "my_commons": MDSInstance(
                    mds_url="",
                    commons_url="",
                    columns_to_fields={},
                ),
            },
            adapter_commons={
                "adapter_commons": AdapterMDSInstance(
                    mds_url="",
                    commons_url="",
                    adapter="icpsr",
                ),
            },
        ),
        "",
        0,
    )


@pytest.mark.asyncio
async def test_filter_entries():
    resp = await filter_entries(
        MDSInstance(
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
            select_field={
                "field_name": "my_field",
                "field_value": 71,
            },
        ),
        [
            {
                "short_name": {"gen3_discovery": {"my_field": 71}},
            },
            {
                "short_name": {"gen3_discovery": {"my_field": 0}},
            },
        ],
    )
    assert resp == [{"short_name": {"gen3_discovery": {"my_field": 71}}}]


def test_parse_config_from_file():
    with NamedTemporaryFile(mode="w+", delete=False) as fp:
        json.dump(
            {
                "gen3_commons": {
                    "mycommons": {
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
                    },
                },
                "adapter_commons": {
                    "non-gen3": {
                        "mds_url": "http://non-gen3",
                        "commons_url": "non-gen3",
                        "adapter": "icpsr",
                    }
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
            },
            {
                "non-gen3": AdapterMDSInstance(
                    "http://non-gen3",
                    "non-gen3",
                    "icpsr",
                )
            },
        ).to_json()
    )
