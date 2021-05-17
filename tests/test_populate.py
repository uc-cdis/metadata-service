import pytest
from argparse import Namespace
from mds.populate import parse_args, main, filter_entries
import mds.agg_mds.mds
from mds.agg_mds import mds
from mds.agg_mds.commons import MDSInstance, Commons
from mds.agg_mds.redis_cache import redis_cache, RedisCache
import respx
from unittest.mock import patch
import fakeredis


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
async def test_main(mock_redis_cache):
    def mock_pull_mds(url):
        return {
            "thing": {
                "gen3_discovery": {
                    "commons_name": "my_commons",
                    "tags": [{"category": "tag_category", "name": "tag_name"}],
                    "_subjects_count": 30,
                }
            }
        }

    with patch("mds.populate.pull_mds", mock_pull_mds):
        await main(
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
            "",
            0,
        )

    with patch("mds.config.USE_AGG_MDS", False):
        with pytest.raises(SystemExit) as pytest_wrapped_e:
            await main(None, "", 0)
        assert pytest_wrapped_e.type == SystemExit
        assert pytest_wrapped_e.value.code == 1


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
