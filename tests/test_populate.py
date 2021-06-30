import pytest
from argparse import Namespace
from mds.populate import parse_args, main, insert_data, filter_entries
from mds.agg_mds.commons import MDSInstance, Commons
import respx
from unittest.mock import patch, MagicMock
from conftest import AsyncMock


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
async def test_main():
    with patch("mds.config.USE_AGG_MDS", False):
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            await main(None, "", 0)
        assert pytest_wrapped_e.type == SystemExit
        assert pytest_wrapped_e.value.code == 1


@pytest.mark.asyncio
async def test_insert_data():
    patch(
        "mds.populate.pull_mds",
        MagicMock(
            return_value={
                "study1": {
                    "gen3_discovery": {
                        "tags": [{"category": "tag_category", "name": "tag_name"}],
                    },
                },
                "study2": {
                    "gen3_discovery": {
                        "tags": [{"category": "tag_category", "name": "tag_name"}],
                    },
                },
            }
        ),
    ).start()
    patch("mds.agg_mds.datastore.update_metadata", AsyncMock(return_value=None)).start()

    await insert_data(
        Commons(
            {
                "my_commons": MDSInstance(
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
            ["_subjects_count"],
        ),
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
