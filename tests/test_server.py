import pytest
import starlette
from requests import ReadTimeout


def test_version(client):
    client.get("/version").raise_for_status()


@pytest.mark.skipif(
    starlette.__version__ < "0.13",
    reason="https://github.com/encode/starlette/pull/751",
)
def test_lost_client(client):
    with pytest.raises(ReadTimeout):
        client.post(
            "/metadata",
            json=[dict(guid=f"tlc_{i}", data=dict(tlc=1)) for i in range(1024)],
            timeout=0.01,
        )
    assert len(client.get("/metadata?limit=1024&tlc=1").json()) < 1024
