import importlib
import time

import pytest
import starlette
from asyncpg import InvalidCatalogNameError
from requests import ReadTimeout
from starlette.testclient import TestClient

from mds import config


def test_version(client):
    client.get("/version").raise_for_status()


@pytest.mark.skipif(
    starlette.__version__ < "0.14",
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


def test_status(client):
    client.get("/_status").raise_for_status()


def test_wait_for_db(monkeypatch):
    monkeypatch.setenv("DB_CONNECT_RETRIES", "0")
    monkeypatch.setenv("DB_DATABASE", "non_exist")
    importlib.reload(config)

    from mds.app import app

    start = time.time()
    with pytest.raises(InvalidCatalogNameError, match="non_exist"):
        with TestClient(app) as client:
            client.get("/_status").raise_for_status()
    assert time.time() - start < 1

    monkeypatch.setenv("DB_CONNECT_RETRIES", "2")
    importlib.reload(config)

    start = time.time()
    with pytest.raises(InvalidCatalogNameError, match="non_exist"):
        with TestClient(app) as client:
            client.get("/_status").raise_for_status()
    assert time.time() - start > 1
