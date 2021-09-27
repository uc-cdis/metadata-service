import gino
import pytest
from unittest.mock import patch
from conftest import AsyncMock


def test_status_success(client):
    patch(
        "mds.main.aggregate_datastore.get_status", AsyncMock(return_value="some status")
    ).start()
    patch("mds.main.db.scalar", AsyncMock(return_value="some time")).start()

    resp = client.get("/_status")
    resp.raise_for_status()
    assert resp.status_code == 200
    assert resp.json() == {
        "status": "OK",
        "timestamp": "some time",
        "aggregate_metadata_enabled": True,
    }


def test_status_aggregate_error(client):
    patch(
        "mds.main.aggregate_datastore.get_status",
        AsyncMock(side_effect=Exception("some error")),
    ).start()
    patch("mds.main.db.scalar", AsyncMock(return_value="some time")).start()

    try:
        resp = client.get("/_status")
        resp.raise_for_status()
    except:
        assert resp.status_code == 500
        assert resp.json() == {
            "detail": {"message": "aggregate datastore offline", "code": 500}
        }
