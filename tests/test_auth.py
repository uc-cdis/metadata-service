import time
from unittest.mock import AsyncMock, patch
import uuid

import httpx
import pytest
import respx

from mds import config
from conftest import TEST_ISSUER


def make_request(client, endpoint, token):
    """
    Make a request to an endpoint that hands the token off to an authorization service,
    and return the response along with the mocked authorization check.

    - "upload": the token is used to request an upload URL from Fence
    - "admin": the token is used to check admin access in Arborist
    """
    headers = {"Authorization": f"bearer {token}"}

    if endpoint == "upload":
        guid = str(uuid.uuid4())
        with respx.mock(assert_all_called=False) as respx_mock:
            authz_check_mock = respx_mock.post(
                config.DATA_ACCESS_SERVICE_ENDPOINT.rstrip("/") + "/data/upload"
            ).mock(
                return_value=httpx.Response(
                    status_code=200, json={"guid": guid, "url": "https://signed.url"}
                )
            )
            data = {
                "file_name": "test.txt",
                "authz": {"version": 0, "resource_paths": ["/programs/DEV"]},
            }
            res = client.post("/objects/upload", json=data, headers=headers)
        if res.status_code == 201:
            client.delete(f"/metadata/{guid}")
        return res, authz_check_mock

    if endpoint == "admin":
        # disable the authorization bypass so the token is checked
        with patch.object(config, "DEBUG", False), patch(
            "mds.admin_login.arborist.auth_request", AsyncMock(return_value=True)
        ) as authz_check_mock:
            res = client.get("/metadata_index", headers=headers)
        return res, authz_check_mock

    raise ValueError(f"Unknown endpoint: {endpoint}")


@pytest.mark.parametrize("endpoint", ["upload", "admin"])
@pytest.mark.parametrize("test_case", ["match", "mismatch"])
def test_token_audience_check(client, generate_signed_token, endpoint, test_case):
    """
    Test that a token's audience is validated before it is handed off to the authorization service.
    """
    # `gen3` is the default audience for Gen3 Fence tokens
    token_aud = "gen3" if test_case == "match" else "WORKFLOW"
    token = generate_signed_token(aud=token_aud)

    res, authz_check_mock = make_request(client, endpoint, token)

    if test_case == "match":
        assert res.status_code in (200, 201), res.text
        assert authz_check_mock.called
    else:
        assert res.status_code == 401, res.text
        assert not authz_check_mock.called


@pytest.mark.parametrize("endpoint", ["upload", "admin"])
@pytest.mark.parametrize("test_case", ["match", "mismatch"])
def test_token_issuer_check(client, generate_signed_token, endpoint, test_case):
    """
    Test that a token's issuer is validated before it is handed off to the authorization service.
    """
    token_iss = (
        TEST_ISSUER if test_case == "match" else "https://other-issuer.example.com/user"
    )
    token = generate_signed_token(iss=token_iss)

    res, authz_check_mock = make_request(client, endpoint, token)

    if test_case == "match":
        assert res.status_code in (200, 201), res.text
        assert authz_check_mock.called
    else:
        assert res.status_code == 401, res.text
        assert not authz_check_mock.called


@pytest.mark.parametrize("endpoint", ["upload", "admin"])
@pytest.mark.parametrize("test_case", ["expired", "valid"])
def test_token_expiration_check(client, generate_signed_token, endpoint, test_case):
    """
    Test that a token's expiration is validated before it is handed off to the authorization
    service.
    """
    now = int(time.time())
    token_exp = now + 300 if test_case == "valid" else now - 300
    token = generate_signed_token(exp=token_exp)

    res, authz_check_mock = make_request(client, endpoint, token)

    if test_case == "valid":
        assert res.status_code in (200, 201), res.text
        assert authz_check_mock.called
    else:
        assert res.status_code == 401, res.text
        assert not authz_check_mock.called
