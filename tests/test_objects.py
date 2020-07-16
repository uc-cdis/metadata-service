import pytest

import httpx
import respx
from fastapi import HTTPException
from starlette.config import environ
from starlette.status import (
    HTTP_201_CREATED,
    HTTP_409_CONFLICT,
    HTTP_400_BAD_REQUEST,
    HTTP_401_UNAUTHORIZED,
    HTTP_403_FORBIDDEN,
    HTTP_500_INTERNAL_SERVER_ERROR,
)

from mds import config


def test_create_no_auth_header(client, valid_upload_file_patcher):
    """
    Test that no token results in 401
    """
    valid_upload_file_patcher["access_token_mock"].side_effect = Exception(
        "token not defined"
    )
    data = {
        "file_name": "test.txt",
        "authz": {"version": 0, "resource_paths": ["/programs/DEV"]},
        "aliases": ["abcdefg"],
        "metadata": {"foo": "bar"},
    }
    resp = client.post("/objects", json=data)
    assert str(resp.status_code) == "401"


def test_create_invalid_token(client, valid_upload_file_patcher):
    """
    Test that a bad token results in 401
    """
    fake_jwt = "1.2.3"
    valid_upload_file_patcher["access_token_mock"].side_effect = HTTPException(
        HTTP_403_FORBIDDEN, "bad token"
    )
    data = {
        "file_name": "test.txt",
        "authz": {"version": 0, "resource_paths": ["/programs/DEV"]},
        "aliases": ["abcdefg"],
        "metadata": {"foo": "bar"},
    }

    resp = client.post(
        "/objects", json=data, headers={"Authorization": f"bearer {fake_jwt}"}
    )
    assert str(resp.status_code) == "401"


@respx.mock
@pytest.mark.parametrize(
    "data",
    [
        # invalid authz version with unknown fields
        {
            "file_name": "test.txt",
            "authz": {
                "version": 1,
                "new_auth": {
                    "source": "foobar",
                    "acl": "(1 AND 2) OR ((3 AND 4) OR 5)",
                },
            },
            "aliases": ["abcdefg"],
            "metadata": {"foo": "bar"},
        }
    ],
)
def test_authz_version_not_supported(client, valid_upload_file_patcher, data):
    """
    Test create /objects response when the authz provided is not supported.
    Assume valid input, ensure correct response.
    """
    fake_jwt = "1.2.3"
    resp = client.post(
        "/objects", json=data, headers={"Authorization": f"bearer {fake_jwt}"}
    )

    assert str(resp.status_code) == "400"
    assert resp.json().get("detail")
    assert not resp.json().get("guid")
    assert not resp.json().get("upload_url")
    assert not resp.json().get("aliases")
    assert not resp.json().get("metadata")

    assert not valid_upload_file_patcher["data_upload_mock"].called
    assert not valid_upload_file_patcher["create_aliases_mock"].called


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

    assert "_resource_paths" in resp.json().get("metadata")
    assert "_uploader_id" in resp.json().get("metadata")
    assert "_upload_status" in resp.json().get("metadata")
    assert client.get(f"/metadata/{resp.json().get('guid')}").json() == resp.json().get(
        "metadata"
    )

    assert valid_upload_file_patcher["data_upload_mock"].called


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
        }
    ],
)
def test_create_no_access_to_upload(client, no_authz_upload_file_patcher, data):
    """
    Test create /objects response for a user WITHOUT authorization to upload.
    Assume valid input, ensure correct response.

    NOTE: the no_authz_upload_file_patcher fixture forces a 403 from external api call
          for uploading data
    """
    fake_jwt = "1.2.3"
    resp = client.post(
        "/objects", json=data, headers={"Authorization": f"bearer {fake_jwt}"}
    )

    assert str(resp.status_code) == "403"
    assert resp.json().get("detail")
    assert not resp.json().get("guid")
    assert not resp.json().get("upload_url")
    assert not resp.json().get("aliases")
    assert not resp.json().get("metadata")

    assert no_authz_upload_file_patcher["data_upload_mock"].called
    assert not no_authz_upload_file_patcher["create_aliases_mock"].called


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
        # no aliases
        {
            "file_name": "test.txt",
            "authz": {"version": 0, "resource_paths": ["/programs/DEV"]},
            "metadata": {"foo": "bar"},
        },
    ],
)
def test_create_no_access_to_create_aliases(
    client, no_authz_create_aliases_patcher, data
):
    """
    Test create /objects response for a user WITHOUT authorization to create aliases
    when the aliases.

    NOTE: the no_authz_create_aliases_patcher fixture forces a 403 from external api call
          for uploading data
    """
    fake_jwt = "1.2.3"
    resp = client.post(
        "/objects", json=data, headers={"Authorization": f"bearer {fake_jwt}"}
    )

    if data.get("aliases"):
        assert str(resp.status_code) == "403"
        assert resp.json().get("detail")
        assert not resp.json().get("guid")
        assert not resp.json().get("upload_url")
        assert not resp.json().get("aliases")
        assert not resp.json().get("metadata")

        assert no_authz_create_aliases_patcher["data_upload_mock"].called
        assert no_authz_create_aliases_patcher["create_aliases_mock"].called
    else:
        # in this case we expect a successful response b/c no aliases were requested
        assert str(resp.status_code).startswith("20")
        assert resp.json().get("aliases") == data.get("aliases", [])
        for key, value in data.get("metadata", {}).items():
            assert resp.json().get("metadata").get(key) == value

        assert resp.json().get("guid") == no_authz_create_aliases_patcher[
            "data_upload_mocked_reponse"
        ].get("guid")
        assert resp.json().get("upload_url") == no_authz_create_aliases_patcher[
            "data_upload_mocked_reponse"
        ].get("url")

        assert client.get(
            f"/metadata/{resp.json().get('guid')}"
        ).json() == resp.json().get("metadata")

        assert no_authz_create_aliases_patcher["data_upload_mock"].called
        assert not no_authz_create_aliases_patcher["create_aliases_mock"].called


# api call fails with 500
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
        }
    ],
)
def test_external_api_upload_failure(client, upload_failure_file_patcher, data):
    """
    Test create /objects response when external api returns a failure.
    Assume valid input, ensure correct response.

    NOTE: the upload_failure_file_patcher fixture forces a 500 from external api call
          for uploading data
    """
    fake_jwt = "1.2.3"
    resp = client.post(
        "/objects", json=data, headers={"Authorization": f"bearer {fake_jwt}"}
    )

    assert str(resp.status_code) == "500"
    assert resp.json().get("detail")
    assert not resp.json().get("guid")
    assert not resp.json().get("upload_url")
    assert not resp.json().get("aliases")
    assert not resp.json().get("metadata")

    assert upload_failure_file_patcher["data_upload_mock"].called
    assert not upload_failure_file_patcher["create_aliases_mock"].called


# api call fails with 500
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
        }
    ],
)
def test_external_api_aliases_failure(client, create_aliases_failure_patcher, data):
    """
    Test create /objects response when external api returns a failure.
    Assume valid input, ensure correct response.

    NOTE: the create_aliases_failure_patcher fixture forces a 500 from external api call
          for creating alises
    """
    fake_jwt = "1.2.3"
    resp = client.post(
        "/objects", json=data, headers={"Authorization": f"bearer {fake_jwt}"}
    )

    assert str(resp.status_code) == "500"
    assert resp.json().get("detail")
    assert not resp.json().get("guid")
    assert not resp.json().get("upload_url")
    assert not resp.json().get("aliases")
    assert not resp.json().get("metadata")

    assert create_aliases_failure_patcher["data_upload_mock"].called
    assert create_aliases_failure_patcher["create_aliases_mock"].called
