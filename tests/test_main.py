from unittest.mock import AsyncMock, patch
import re


def test_status_success(client):
    patch(
        "mds.main.aggregate_datastore.get_status", AsyncMock(return_value="some status")
    ).start()
    with patch("mds.main.get_data_access_layer", AsyncMock()):
        resp = client.get("/_status")
        resp.raise_for_status()
        assert resp.status_code == 200

        body = resp.json()
        assert body["status"] == "OK"
        assert body["aggregate_metadata_enabled"] is True

        # ISO timestamp regex
        iso_timestamp_regex = (
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})"
        )
        assert isinstance(body["timestamp"], str)
        assert re.fullmatch(iso_timestamp_regex, body["timestamp"])


def test_status_aggregate_error(client):
    patch(
        "mds.main.aggregate_datastore.get_status",
        AsyncMock(side_effect=Exception("some error")),
    ).start()
    with patch("mds.main.get_data_access_layer", AsyncMock(return_value="some time")):
        try:
            resp = client.get("/_status")
            resp.raise_for_status()
        except Exception:
            assert resp.status_code == 500
            assert resp.json() == {
                "detail": {"message": "aggregate datastore offline", "code": 500}
            }
