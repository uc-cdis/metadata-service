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

    fake_guid = "dg.hello/test_guid"
    resp = client.post(f"/objects/{fake_guid}", json=data)
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
        assert resp.json().get("metadata", {}).get(key) == value

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
    Test create /objects response for a user WITHOUT authorization to create aliases.

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
            assert resp.json().get("metadata", {}).get(key) == value

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


@respx.mock
@pytest.mark.parametrize(
    "data",
    [
        {
            "file_name": "test.txt",
            "authz": {"version": 0, "resource_paths": ["/programs/DEV"]},
            "aliases": ["alias1", "alias2", "alias1"],
            "metadata": {"foo": "bar"},
        },
    ],
)
def test_create_duplicate_aliases(client, create_aliases_duplicate_patcher, data):
    """
    Test create /objects response for a user with valid authorization and
    valid input, but some aliases already exist. We get a 409 from
    indexd's alias creation endpoint. The MDS endpoint should return 409.
    """
    fake_jwt = "1.2.3"
    fake_guid = create_aliases_duplicate_patcher["data_upload_mocked_reponse"].get(
        "guid"
    )

    resp = client.post(
        "/objects", json=data, headers={"Authorization": f"bearer {fake_jwt}"}
    )

    assert resp.status_code == 409
    assert resp.json().get("detail")
    assert not resp.json().get("guid")
    assert not resp.json().get("upload_url")
    assert not resp.json().get("aliases")
    assert not resp.json().get("metadata")

    assert create_aliases_duplicate_patcher["data_upload_mock"].called


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


@respx.mock
@pytest.mark.parametrize(
    "data",
    [
        # all valid fields
        {
            "file_name": "test.txt",
            "aliases": ["abcdefg"],
            "metadata": {"foo": "bar"},
        },
        # all valid fields (multiple aliases and metadata keys)
        {
            "file_name": "test.txt",
            "aliases": ["abcdefg", "123456"],
            "metadata": {"foo": "bar", "fizz": "buzz"},
        },
        # no aliases
        {
            "file_name": "test.txt",
            "metadata": {"foo": "bar"},
        },
        # no metadata
        {
            "file_name": "test.txt",
            "aliases": ["abcdefg"],
        },
        # no aliases or metadata
        {
            "file_name": "test.txt",
        },
    ],
)
def test_create_for_guid(client, valid_upload_file_patcher, data):
    """
    Test create /objects/<GUID or alias> for a valid user with authorization
    and valid input, ensure correct response: 200 and metadata if the GUID
    or alias exists in indexd.
    If the key is an indexd alias, the metadata returned should be
    associated with the indexd GUID (did), not the alias itself.
    """
    fake_jwt = "1.2.3"
    guid_or_alias = "test_guid_alias"
    indexd_did = "dg.hello/test_guid"
    indexd_data = {
        "did": indexd_did,
        "rev": "123",
        "file_name": "im_a_blank_record.pfb",
        "acl": ["resource"],
        "authz": ["/path/to/resource"],
    }
    new_version_guid = valid_upload_file_patcher["data_upload_mocked_reponse"].get(
        "guid"
    )
    new_version_data = {
        "did": new_version_guid,
        "rev": "987",
        "file_name": "im_another_blank_record.pfb",
    }

    # mock: creating a new version of "indexd_did" returns "new_version_data"
    indexd_blank_version_mocked_request = respx.post(
        f"{config.INDEXING_SERVICE_ENDPOINT}/index/blank/{indexd_did}",
        status_code=200,
        content=new_version_data,
        alias="indexd_post_blank",
    )

    # mock the request to indexd: GUID or alias found in indexd
    indexd_url = f"{config.INDEXING_SERVICE_ENDPOINT}/{guid_or_alias}"
    indexd_get_mocked_request = respx.get(
        indexd_url, status_code=200, content=indexd_data, alias="indexd_get"
    )
    resp = client.post(
        f"/objects/{guid_or_alias}",
        json=data,
        headers={"Authorization": f"bearer {fake_jwt}"},
    )
    resp.raise_for_status()

    # check response contents
    assert str(resp.status_code).startswith("20")
    assert resp.json().get("aliases") == data.get("aliases", [])
    for key, value in data.get("metadata", {}).items():
        assert resp.json().get("metadata", {}).get(key) == value

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

    assert indexd_get_mocked_request.called
    assert indexd_blank_version_mocked_request.called
    assert valid_upload_file_patcher["data_upload_guid_mock"].called


@respx.mock
@pytest.mark.parametrize(
    "data",
    [
        # all valid fields
        {
            "file_name": "test.txt",
            "aliases": ["abcdefg"],
            "metadata": {"foo": "bar"},
        },
        # all valid fields (multiple aliases and metadata keys)
        {
            "file_name": "test.txt",
            "aliases": ["abcdefg", "123456"],
            "metadata": {"foo": "bar", "fizz": "buzz"},
        },
        # no aliases
        {
            "file_name": "test.txt",
            "metadata": {"foo": "bar"},
        },
        # no metadata
        {
            "file_name": "test.txt",
            "aliases": ["abcdefg"],
        },
        # no aliases or metadata
        {
            "file_name": "test.txt",
        },
    ],
)
def test_create_for_guid_not_found(client, valid_upload_file_patcher, data):
    """
    Test create /objects/<GUID or alias> for a valid user with authorization
    and valid input, ensure correct response: 404 and no metadata if the GUID
    or alias does not exist in indexd.
    """
    fake_jwt = "1.2.3"
    guid_or_alias = "test_guid_alias"
    indexd_did = "dg.hello/test_guid"
    indexd_data = {
        "did": indexd_did,
        "rev": "123",
        "file_name": "im_a_blank_record.pfb",
        "acl": ["resource"],
        "authz": ["/path/to/resource"],
    }
    new_version_guid = valid_upload_file_patcher["data_upload_mocked_reponse"].get(
        "guid"
    )
    new_version_data = {
        "did": new_version_guid,
        "rev": "987",
        "file_name": "im_another_blank_record.pfb",
    }

    # mock: creating a new version of "indexd_did" returns "new_version_data"
    indexd_blank_version_mocked_request = respx.post(
        f"{config.INDEXING_SERVICE_ENDPOINT}/index/blank/{indexd_did}",
        status_code=200,
        content=new_version_data,
        alias="indexd_post_blank",
    )

    # mock the request to indexd: GUID or alias NOT found in indexd
    indexd_url = f"{config.INDEXING_SERVICE_ENDPOINT}/{guid_or_alias}"
    indexd_get_mocked_request = respx.get(
        indexd_url, status_code=404, content=indexd_data, alias="indexd_get"
    )
    resp = client.post(
        f"/objects/{guid_or_alias}",
        json=data,
        headers={"Authorization": f"bearer {fake_jwt}"},
    )

    # check response contents
    assert resp.status_code == 404
    assert resp.json().get("detail")
    assert not resp.json().get("guid")
    assert not resp.json().get("upload_url")
    assert not resp.json().get("aliases")
    assert not resp.json().get("metadata")
    assert indexd_get_mocked_request.called


@respx.mock
@pytest.mark.parametrize(
    "data",
    [
        # all valid fields
        {
            "file_name": "test.txt",
            "aliases": ["abcdefg"],
            "metadata": {"foo": "bar"},
        },
    ],
)
def test_create_for_guid_no_access_to_create_blank_version(
    client, valid_upload_file_patcher, data
):
    """
    Test create /objects/<GUID or alias> for valid input, but a user without
    authorization to create a blank version in indexd. Should return 403.
    """
    fake_jwt = "1.2.3"
    guid_or_alias = "test_guid_alias"
    indexd_did = "dg.hello/test_guid"
    indexd_data = {
        "did": indexd_did,
        "rev": "123",
        "file_name": "im_a_blank_record.pfb",
        "acl": ["resource"],
        "authz": ["/path/to/resource"],
    }

    # mock: creating a new version of "indexd_did" returns 403 unauthorized
    indexd_blank_version_mocked_request = respx.post(
        f"{config.INDEXING_SERVICE_ENDPOINT}/index/blank/{indexd_did}",
        status_code=403,
        alias="indexd_post_blank",
    )

    # mock the request to indexd: GUID or alias found in indexd
    indexd_url = f"{config.INDEXING_SERVICE_ENDPOINT}/{guid_or_alias}"
    indexd_get_mocked_request = respx.get(
        indexd_url, status_code=200, content=indexd_data, alias="indexd_get"
    )

    resp = client.post(
        f"/objects/{guid_or_alias}",
        json=data,
        headers={"Authorization": f"bearer {fake_jwt}"},
    )

    # check response contents
    assert resp.status_code == 403
    assert resp.json().get("detail")
    assert not resp.json().get("guid")
    assert not resp.json().get("upload_url")
    assert not resp.json().get("aliases")
    assert not resp.json().get("metadata")
    assert indexd_get_mocked_request.called

    assert indexd_get_mocked_request.called
    assert indexd_blank_version_mocked_request.called


@respx.mock
@pytest.mark.parametrize(
    "data",
    [
        # all valid fields
        {
            "file_name": "test.txt",
            "aliases": ["abcdefg"],
            "metadata": {"foo": "bar"},
        },
    ],
)
def test_create_for_guid_no_access_to_upload(
    client, no_authz_upload_file_patcher, data
):
    """
    Test create /objects/<GUID or alias> for valid input, but a user without
    authorization to get a presigned URL for upload. Should return 403.
    """
    fake_jwt = "1.2.3"
    guid_or_alias = "test_guid_alias"
    indexd_did = "dg.hello/test_guid"
    indexd_data = {
        "did": indexd_did,
        "rev": "123",
        "file_name": "im_a_blank_record.pfb",
        "acl": ["resource"],
        "authz": ["/path/to/resource"],
    }
    new_version_guid = no_authz_upload_file_patcher["data_upload_mocked_reponse"].get(
        "guid"
    )
    new_version_data = {
        "did": new_version_guid,
        "rev": "987",
        "file_name": "im_another_blank_record.pfb",
    }

    # mock: creating a new version of "indexd_did" returns "new_version_data"
    indexd_blank_version_mocked_request = respx.post(
        f"{config.INDEXING_SERVICE_ENDPOINT}/index/blank/{indexd_did}",
        status_code=200,
        content=new_version_data,
        alias="indexd_post_blank",
    )

    # mock the request to indexd: GUID or alias found in indexd
    indexd_url = f"{config.INDEXING_SERVICE_ENDPOINT}/{guid_or_alias}"
    indexd_get_mocked_request = respx.get(
        indexd_url, status_code=200, content=indexd_data, alias="indexd_get"
    )

    resp = client.post(
        f"/objects/{guid_or_alias}",
        json=data,
        headers={"Authorization": f"bearer {fake_jwt}"},
    )

    # check response contents
    assert resp.status_code == 403
    assert resp.json().get("detail")
    assert not resp.json().get("guid")
    assert not resp.json().get("upload_url")
    assert not resp.json().get("aliases")
    assert not resp.json().get("metadata")
    assert indexd_get_mocked_request.called

    assert indexd_get_mocked_request.called
    assert indexd_blank_version_mocked_request.called


@respx.mock
def test_get_object_in_indexd(client):
    """
    Test the GET object endpoint when the provided key exists in indexd.
    If the key is an indexd alias, the metadata returned should be
    associated with the indexd GUID (did), not the alias itself.
    If the key exists in indexd, the record should be returned regardless
    of a 404 from MDS.
    """
    guid_or_alias = "dg.hello/test_guid"
    indexd_did = "test_did"

    # mock the request to indexd: GUID or alias found in indexd
    indexd_url = f"{config.INDEXING_SERVICE_ENDPOINT}/{guid_or_alias}"
    indexd_data = {"did": indexd_did, "size": 42}
    indexd_get_mocked_request = respx.get(
        indexd_url, status_code=200, content=indexd_data
    )

    # GET an object that exists in indexd but NOT in MDS
    get_object_url = f"/objects/{guid_or_alias}"
    resp = client.get(get_object_url)
    assert indexd_get_mocked_request.called
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"record": indexd_data, "metadata": {}}

    # create metadata for this object
    mds_data = dict(a=1, b=2)
    client.post("/metadata/" + indexd_did, json=mds_data).raise_for_status()

    # GET an object that exists in indexd AND in MDS
    try:
        resp = client.get(get_object_url)
        assert indexd_get_mocked_request.called
        assert resp.status_code == 200, resp.text
        assert resp.json() == {"record": indexd_data, "metadata": mds_data}
    finally:
        client.delete("/metadata/" + indexd_did)


@respx.mock
def test_get_object_not_in_indexd(client):
    """
    Test the GET object endpoint when the provided key does NOT exist
    in indexd, or when indexd errors.
    If the key exists in MDS, the metadata should be returned regardless
    of a non-200 response from indexd.
    """
    guid_or_alias = "dg.hello/test_guid"

    # mock the request to indexd: GUID or alias NOT found in indexd
    indexd_url = f"{config.INDEXING_SERVICE_ENDPOINT}/{guid_or_alias}"
    indexd_get_mocked_request = respx.get(indexd_url, status_code=404)

    # GET an object that exists in NEITHER indexd NOR MDS
    get_object_url = f"/objects/{guid_or_alias}"
    resp = client.get(get_object_url)
    assert indexd_get_mocked_request.called
    assert resp.status_code == 404, resp.text

    # create metadata for this object
    mds_data = dict(a=1, b=2)
    client.post("/metadata/" + guid_or_alias, json=mds_data).raise_for_status()

    try:
        # GET an object that exists in MDS but NOT in indexd
        resp = client.get(get_object_url)
        assert indexd_get_mocked_request.called
        assert resp.status_code == 200, resp.text
        assert resp.json() == {"record": {}, "metadata": mds_data}

        # mock the request to indexd: 500 error from indexd
        respx.clear()
        indexd_get_mocked_request = respx.get(indexd_url, status_code=500)

        # GET an object that exists in MDS, even if indexd failed
        resp = client.get(get_object_url)
        assert indexd_get_mocked_request.called
        assert resp.status_code == 200, resp.text
        assert resp.json() == {"record": {}, "metadata": mds_data}
    finally:
        client.delete("/metadata/" + guid_or_alias)


@respx.mock
def test_get_object_signed_download_url_for_data_access_200(
    client, download_endpoints, signed_url_mock
):
    """
    Test that mds returns a 200 containing the signed download url when the
    data access service download endpoint returns a 200.
    """
    data_access_signed_download_get_request_mock = respx.get(
        download_endpoints["data_access"],
        status_code=200,
        content={"url": signed_url_mock},
    )

    resp = client.get(download_endpoints["mds"])
    assert data_access_signed_download_get_request_mock.called
    assert resp.status_code == 200, resp.text
    resp_json = resp.json()
    assert "url" in resp_json
    assert resp_json["url"] == signed_url_mock


@respx.mock
def test_get_object_signed_download_url_for_data_access_404(
    client, download_endpoints, signed_url_mock
):
    """
    Test that mds returns a 404 when the data access service download endpoint
    returns a 404.
    """
    data_access_signed_download_get_request_mock = respx.get(
        download_endpoints["data_access"], status_code=404
    )

    resp = client.get(download_endpoints["mds"])
    assert data_access_signed_download_get_request_mock.called
    assert resp.status_code == 404, resp.text


@respx.mock
def test_get_object_signed_download_url_for_data_access_401(
    client, download_endpoints, signed_url_mock
):
    """
    Test that mds returns a 403 when the data access service download endpoint
    returns a 401.
    """
    data_access_signed_download_get_request_mock = respx.get(
        download_endpoints["data_access"], status_code=401
    )

    resp = client.get(download_endpoints["mds"])
    assert data_access_signed_download_get_request_mock.called
    assert resp.status_code == 403, resp.text


@respx.mock
def test_get_object_signed_download_url_for_data_access_403(
    client, download_endpoints, signed_url_mock
):
    """
    Test that mds returns a 403 when the data access service download endpoint
    returns a 403.
    """
    data_access_signed_download_get_request_mock = respx.get(
        download_endpoints["data_access"], status_code=403
    )

    resp = client.get(download_endpoints["mds"])
    assert data_access_signed_download_get_request_mock.called
    assert resp.status_code == 403, resp.text


@respx.mock
def test_get_object_signed_download_url_for_data_access_500(
    client, download_endpoints, signed_url_mock
):
    """
    Test that mds returns a 500 when the data access service download endpoint
    returns a 500.
    """
    data_access_signed_download_get_request_mock = respx.get(
        download_endpoints["data_access"], status_code=500
    )

    resp = client.get(download_endpoints["mds"])
    assert data_access_signed_download_get_request_mock.called
    assert resp.status_code == 500, resp.text


@respx.mock
def test_get_object_latest_when_indexd_returns_different_guid_and_different_guid_in_mds(
    client, latest_setup
):
    """
    Test that mds returns a 200 containing an indexd record and mds object
    associated with the guid returned from indexd's latest endpoint (in this
    case, that latest guid returned from indexd is different to the oldest guid
    initially provided to the mds latest endpoint).
    """
    get_indexd_latest_request_mock = respx.get(
        latest_setup["indexd_latest_endpoint_with_oldest_guid"],
        status_code=200,
        content=latest_setup["indexd_latest_record_data"],
    )

    resp = client.get(latest_setup["mds_latest_endpoint_with_oldest_guid"])
    assert get_indexd_latest_request_mock.called
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "record": latest_setup["indexd_latest_record_data"],
        "metadata": latest_setup["mds_objects"]["latest"]["data"],
    }


@respx.mock
def test_get_object_latest_when_indexd_returns_same_guid_and_same_guid_in_mds(
    client, latest_setup
):
    """
    Test that mds returns a 200 containing an indexd record and mds object
    associated with the guid initially provided to the mds latest endpoint (in
    this case, indexd's latest endpoint returns a guid that is the same as the
    one intially provided to the mds latest endpoint).
    """
    get_indexd_latest_request_mock = respx.get(
        latest_setup["indexd_latest_endpoint_with_oldest_guid"],
        status_code=200,
        content=latest_setup["indexd_oldest_record_data"],
    )

    resp = client.get(latest_setup["mds_latest_endpoint_with_oldest_guid"])
    assert get_indexd_latest_request_mock.called
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "record": latest_setup["indexd_oldest_record_data"],
        "metadata": latest_setup["mds_objects"]["oldest"]["data"],
    }


@respx.mock
def test_get_object_latest_when_indexd_returns_guid_not_in_mds(client, latest_setup):
    """
    Test that mds returns a 200 containing an indexd record associated with the
    guid returned from the indexd latest endpoint and an empty metadata object
    (in this case, the indexd latest endpoint returns a guid that is not a key
    in the mds database).
    """
    get_indexd_latest_request_mock = respx.get(
        latest_setup["indexd_latest_endpoint_with_oldest_guid"],
        status_code=200,
        content=latest_setup["indexd_non_mds_record_data"],
    )

    resp = client.get(latest_setup["mds_latest_endpoint_with_oldest_guid"])
    assert get_indexd_latest_request_mock.called
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "record": latest_setup["indexd_non_mds_record_data"],
        "metadata": {},
    }


@respx.mock
def test_get_object_latest_when_indexd_returns_404_but_guid_in_mds(
    client, latest_setup
):
    """
    Test that mds returns a 200 containing an empty indexd record and a
    metadata object associated with the guid/key intially provided to the mds
    latest endpoint (in this case, indexd's latest endpoint returns a 404).
    """
    get_indexd_latest_request_mock = respx.get(
        latest_setup["indexd_latest_endpoint_with_oldest_guid"],
        status_code=404,
    )

    resp = client.get(latest_setup["mds_latest_endpoint_with_oldest_guid"])
    assert get_indexd_latest_request_mock.called
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "record": {},
        "metadata": latest_setup["mds_objects"]["oldest"]["data"],
    }


@respx.mock
def test_get_object_latest_when_indexd_returns_500_but_guid_in_mds(
    client, latest_setup
):
    """
    Test that mds returns a 200 containing an empty indexd record and a
    metadata object associated with the guid/key intially provided to the mds
    latest endpoint (in this case, indexd's latest endpoint returns a 500).
    """
    get_indexd_latest_request_mock = respx.get(
        latest_setup["indexd_latest_endpoint_with_oldest_guid"],
        status_code=500,
    )

    resp = client.get(latest_setup["mds_latest_endpoint_with_oldest_guid"])
    assert get_indexd_latest_request_mock.called
    assert resp.status_code == 200, resp.text
    assert resp.json() == {
        "record": {},
        "metadata": latest_setup["mds_objects"]["oldest"]["data"],
    }


@respx.mock
def test_get_object_latest_when_indexd_returns_404_and_guid_not_in_mds(
    client, latest_setup
):
    """
    Test that mds returns a 404 when indexd's latest endpoint returns a 404 and
    the guid/key intially provided to the mds latest endpoint is not in the mds
    database.
    """
    get_indexd_latest_request_mock = respx.get(
        latest_setup["indexd_latest_endpoint_with_non_mds_guid"],
        status_code=404,
    )
    resp = client.get(latest_setup["mds_latest_endpoint_with_non_mds_guid"])
    assert get_indexd_latest_request_mock.called
    assert resp.status_code == 404, resp.text


@respx.mock
def test_delete_object_when_fence_returns_204(client, valid_upload_file_patcher):
    """
    Test the DELETE endpoint when fence returns a 204 for specified guid.
    Should proxy to fence's DELETE /data/file_id endpoint then delete metadata.
    A 204 response from fence implies delete permissions on guid.
    """
    file_data = {
        "file_name": "test.txt",
        "authz": {"version": 0, "resource_paths": ["/programs/DEV"]},
        "aliases": ["abcdefg"],
        "metadata": {"foo": "bar"},
    }
    created_guid = client.post(
        "/objects", json=file_data, headers={"Authorization": f"bearer fake_jwt"}
    ).json()["guid"]

    fence_delete_mock = respx.delete(
        f"{config.DATA_ACCESS_SERVICE_ENDPOINT}/data/{created_guid}", status_code=204
    )

    delete_response = client.delete(f"/objects/{created_guid}")
    assert delete_response.status_code == 204
    assert fence_delete_mock.called
    get_metadata_response = client.get(f"/metadata/{created_guid}")
    assert get_metadata_response.status_code == 404


@respx.mock
def test_delete_object_when_fence_returns_403(client, valid_upload_file_patcher):
    """
    Test the DELETE endpoint when fence returns a 403 for specified guid.
    Should proxy to fence's DELETE /data/file_id endpoint and not delete metadata.
    A 403 response from fence implies insufficient permissions to delete metadata.
    """
    file_data = {
        "file_name": "test.txt",
        "authz": {"version": 0, "resource_paths": ["/programs/DEV"]},
        "aliases": ["abcdefg"],
        "metadata": {"foo": "bar"},
    }
    created_guid = client.post(
        "/objects", json=file_data, headers={"Authorization": f"bearer fake_jwt"}
    ).json()["guid"]

    fence_delete_mock = respx.delete(
        f"{config.DATA_ACCESS_SERVICE_ENDPOINT}/data/{created_guid}",
        status_code=403,
        content={"err": "mocked authentication error from fence"},
    )

    delete_response = client.delete(f"/objects/{created_guid}")
    assert delete_response.status_code == 403
    assert fence_delete_mock.called
    get_metadata_response = client.get(f"/metadata/{created_guid}")
    assert get_metadata_response.status_code == 200


@respx.mock
def test_delete_object_when_fence_returns_500(client, valid_upload_file_patcher):
    """
    Test the DELETE endpoint when fence returns a 500 for specified guid.
    Should proxy to fence's DELETE /data/file_id endpoint and not delete metadata.
    """
    file_data = {
        "file_name": "test.txt",
        "authz": {"version": 0, "resource_paths": ["/programs/DEV"]},
        "aliases": ["abcdefg"],
        "metadata": {"foo": "bar"},
    }
    created_guid = client.post(
        "/objects", json=file_data, headers={"Authorization": f"bearer fake_jwt"}
    ).json()["guid"]

    fence_delete_mock = respx.delete(
        f"{config.DATA_ACCESS_SERVICE_ENDPOINT}/data/{created_guid}",
        status_code=500,
        content={"err": "mocked internal server error from fence"},
    )

    delete_response = client.delete(f"/objects/{created_guid}")
    assert delete_response.status_code == 500
    assert fence_delete_mock.called
    get_metadata_response = client.get(f"/metadata/{created_guid}")
    assert get_metadata_response.status_code == 200
