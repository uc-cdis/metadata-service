import time

import httpx
import pytest
import starlette
from requests import ReadTimeout
from starlette.testclient import TestClient

from mds.main import get_app


def test_version(client):
    client.get("/version").raise_for_status()


@pytest.mark.skipif(
    starlette.__version__ < "0.14",
    reason="https://github.com/encode/starlette/pull/751",
)
@pytest.mark.skip(
    reason="Above mentioned PR has not been merged, therefore the feature that this test depends on is not in starlette>=0.14",
)
def test_lost_client(client):
    with pytest.raises(ReadTimeout):
        client.post(
            "/metadata",
            json=[dict(guid=f"tlc_{i}", data=dict(tlc=1)) for i in range(1024)],
            timeout=0.01,
        )
    assert len(client.get("/metadata?limit=1024&tlc=1").json()) < 1024


def test_status(client):
    try:
        client.get("/_status").raise_for_status()
    except Exception:
        pass


def test_wait_for_db(monkeypatch):
    monkeypatch.setenv("DB_DATABASE", "non_exist")
    monkeypatch.setenv(
        "DB_DSN", "postgresql+asyncpg://postgres@localhost:5432/non_exist"
    )
    monkeypatch.setenv("DB_RETRY_LIMIT", "0")

    start = time.time()
    with pytest.raises(httpx.HTTPStatusError) as excinfo:
        with TestClient(get_app()) as client:
            client.get("/_status").raise_for_status()
    assert excinfo.value.response.status_code == 500
    assert time.time() - start < 1
