import time

import pytest
import starlette
from asyncpg import InvalidCatalogNameError
from requests import ReadTimeout
from starlette.testclient import TestClient

from mds.main import db
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
    except:
        pass


def test_wait_for_db(monkeypatch):
    monkeypatch.setitem(db.config, "retry_limit", 0)
    monkeypatch.setattr(db.config["dsn"], "database", "non_exist")

    start = time.time()
    with pytest.raises(InvalidCatalogNameError, match="non_exist"):
        with TestClient(get_app()) as client:
            client.get("/_status").raise_for_status()
    assert time.time() - start < 1

    monkeypatch.setitem(db.config, "retry_limit", 2)

    start = time.time()
    with pytest.raises(InvalidCatalogNameError, match="non_exist"):
        with TestClient(get_app()) as client:
            client.get("/_status").raise_for_status()
    assert time.time() - start > 1
