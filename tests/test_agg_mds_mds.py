import httpx
import respx
from tenacity import RetryError, wait_none
from mds.agg_mds.mds import pull_mds


@respx.mock
def test_pull_mds():
    mock_route1 = respx.get(
        "http://commons1/mds/metadata?data=True&_guid_type=discovery_metadata&limit=2&offset=0",
        content={
            "commons1": {"gen3_discovery": {}},
            "commons2": {"gen3_discovery": {}},
        },
    )
    mock_route2 = respx.get(
        "http://commons1/mds/metadata?data=True&_guid_type=discovery_metadata&limit=2&offset=2",
        content={"commons3": {"gen3_discovery": {}}},
    )

    results = pull_mds("http://commons1", "discovery_metadata", 2)
    assert mock_route1.called
    assert mock_route2.called
    assert results == {
        "commons1": {"gen3_discovery": {}},
        "commons2": {"gen3_discovery": {}},
        "commons3": {"gen3_discovery": {}},
    }

    # changed
    respx.get(
        "http://commons2/mds/metadata?data=True&_guid_type=discovery_metadata&limit=2&offset=0",
        content={},
        status_code=403,
    )
    results = pull_mds("http://commons2", "discovery_metadata", 2)
    assert results == {}

    try:
        pull_mds.retry.wait = wait_none()

        respx.get(
            "http://commons3/mds/metadata?data=True&_guid_type=discovery_metadata&limit=2&offset=0",
            content=httpx.TimeoutException,
        )
        pull_mds("http://commons3", "discovery_metadata", 2)
    except Exception as exc:
        assert isinstance(exc, RetryError) == True
