import pytest
from mds.agg_mds.mds import pull_mds
import respx
from httpx import Response


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

    with pytest.raises(ValueError) as excinfo:
        respx.get(
            "http://commons2/mds/metadata?data=True&_guid_type=discovery_metadata&limit=2&offset=0",
            content={},
            status_code=403,
        )
        results = pull_mds("http://commons2", "discovery_metadata", 2)
    assert "An error occurred while requesting" in str(excinfo.value)
