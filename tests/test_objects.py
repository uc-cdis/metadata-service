import pytest

import httpx
import respx
from fastapi import HTTPException
from starlette.status import (
    HTTP_201_CREATED,
    HTTP_409_CONFLICT,
    HTTP_400_BAD_REQUEST,
    HTTP_401_UNAUTHORIZED,
    HTTP_403_FORBIDDEN,
    HTTP_500_INTERNAL_SERVER_ERROR,
)


@respx.mock
@pytest.mark.parametrize(
    "data",
    [
        # all valid fields
        {
            "file_name": "test.txt",
            "authz": {"version": 0, "resource_paths": ["/programs/DEV"]},
            "aliases": ["abcdefg"],
            "metadata": {"foo": "bar"},
        },
        # all valid fields (multiple resource paths, aliases, and metadata keys)
        {
            "file_name": "test.txt",
            "authz": {
                "version": 0,
                "resource_paths": ["/programs/DEV", "/programs/test"],
            },
            "aliases": ["abcdefg", "123456"],
            "metadata": {"foo": "bar", "fizz": "buzz"},
        },
        # no aliases
        {
            "file_name": "test.txt",
            "authz": {"version": 0, "resource_paths": ["/programs/DEV"]},
            "metadata": {"foo": "bar"},
        },
        # no metadata
        {
            "file_name": "test.txt",
            "authz": {"version": 0, "resource_paths": ["/programs/DEV"]},
            "aliases": ["abcdefg"],
        },
        # no aliases or metadata
        {
            "file_name": "test.txt",
            "authz": {"version": 0, "resource_paths": ["/programs/DEV"]},
        },
    ],
)
def test_create(client, valid_upload_file_patcher, data):
    """
    Test create /objects response for a valid user with authorization and
    valid input, ensure correct response.
    """
    fake_jwt = "1.2.3"
    resp = client.post(
        "/objects", json=data, headers={"Authorization": f"bearer {fake_jwt}"}
    )
    resp.raise_for_status()

    assert str(resp.status_code).startswith("20")
    assert resp.json().get("aliases") == data.get("aliases", [])
    for key, value in data.get("metadata", {}).items():
        assert resp.json().get("metadata").get(key) == value

    assert resp.json().get("guid") == valid_upload_file_patcher[
        "data_upload_mocked_reponse"
    ].get("guid")
    assert resp.json().get("upload_url") == valid_upload_file_patcher[
        "data_upload_mocked_reponse"
    ].get("url")

    assert client.get(f"/metadata/{resp.json().get('guid')}").json() == resp.json().get(
        "metadata"
    )

    assert valid_upload_file_patcher["data_upload_mock"].called


# no access
# some access
# fence call fails
# metadata update fails?


def test_create_invalid_token(client, valid_upload_file_patcher):
    fake_jwt = "1.2.3"
    valid_upload_file_patcher["access_token_mock"].side_effect = HTTPException(
        HTTP_403_FORBIDDEN, "bad token"
    )

    with pytest.raises(Exception) as exc:
        resp = client.post(
            "/objects", json=data, headers={"Authorization": f"bearer {fake_jwt}"}
        )
        resp.raise_for_status()
        assert str(resp.status_code) == "403"


def test_create_no_auth_header():
    data = {
        "file_name": "test.txt",
        "authz": {"version": 0, "resource_paths": ["/programs/DEV"]},
        "aliases": ["abcdefg"],
        "metadata": {"foo": "bar"},
    }
    with pytest.raises(Exception) as exc:
        resp = client.post("/objects", json=data)
        resp.raise_for_status()
        assert str(resp.status_code) == "403"
