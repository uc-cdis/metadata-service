from unittest import mock
import pytest
import respx
from unittest.mock import MagicMock, patch

from fastapi import Depends, HTTPException, Security
from fastapi.security import (
    HTTPAuthorizationCredentials,
    HTTPBasic,
    HTTPBasicCredentials,
    HTTPBearer,
)

from gen3authz.client.arborist.async_client import ArboristClient
from gen3authz.client.arborist.errors import ArboristError
from mds import config
from mds.objects import FORBIDDEN_IDS
from mds import admin_login


@respx.mock
@pytest.mark.asyncio
async def test_login(authz_request_patcher):
    """
    Test admin_login for user with valid token.
    """
    if config.DEBUG:
        config_debug_save = config.DEBUG
        config.DEBUG = False
    else:
        config_debug_save = False

    credentials = HTTPBasicCredentials(username="foo", password="bar")

    # valid token
    resp = await admin_login.admin_required(
        credentials, authz_request_patcher["access_token_mock"]
    )
    assert not resp
    assert authz_request_patcher["auth_request_mock"].called

    if not config.DEBUG and config_debug_save:
        config.DEBUG = config_debug_save


@respx.mock
@pytest.mark.asyncio
async def test_login_access_error(authz_request_false_patcher):
    """
    Test admin_login for a user with missing/bad token and check for 403 errors.
    """
    if config.DEBUG:
        config_debug_save = config.DEBUG
        config.DEBUG = False
    else:
        config_debug_save = False

    credentials = HTTPBasicCredentials(username="foo", password="bar")

    # token = None
    with pytest.raises(HTTPException) as e:
        resp = await admin_login.admin_required(credentials, None)
    assert e.value.status_code == 403
    assert e.value.detail == "Forbidden"
    assert not authz_request_false_patcher["auth_request_mock"].called

    # authz request returns False
    with pytest.raises(HTTPException) as e:
        resp = await admin_login.admin_required(
            credentials, authz_request_false_patcher["access_token_mock"]
        )
    assert e.value.status_code == 403
    assert e.value.detail == "Forbidden"
    assert authz_request_false_patcher["auth_request_mock"].called

    if not config.DEBUG and config_debug_save:
        config.DEBUG = config_debug_save


@respx.mock
@pytest.mark.asyncio
async def test_login_no_access(authz_request_unauthorized_patcher):
    """
    Test admin_login with Arborist Error 401 - Unauthorized.
    """
    if config.DEBUG:
        config_debug_save = config.DEBUG
        config.DEBUG = False
    else:
        config_debug_save = False

    # Arborist Error 401 - Unauthorized
    credentials = HTTPBasicCredentials(username="foo", password="bar")
    with pytest.raises(HTTPException) as e:
        resp = await admin_login.admin_required(
            credentials, authz_request_unauthorized_patcher["access_token_mock"]
        )
    assert e.value.status_code == 401
    assert e.value.detail == "Unauthorized"
    assert authz_request_unauthorized_patcher["auth_request_mock"].called

    if not config.DEBUG and config_debug_save:
        config.DEBUG = config_debug_save


@respx.mock
@pytest.mark.asyncio
async def test_login_forbidden_access(authz_request_forbidden_patcher):
    """
    Test admin_login with Arborist Error 403 - Forbidden.
    """
    if config.DEBUG:
        config_debug_save = config.DEBUG
        config.DEBUG = False
    else:
        config_debug_save = False

    # Arborist Error 403 - Forbidden
    credentials = HTTPBasicCredentials(username="foo", password="bar")
    with pytest.raises(HTTPException) as e:
        resp = await admin_login.admin_required(
            credentials, authz_request_forbidden_patcher["access_token_mock"]
        )
    assert e.value.status_code == 403
    assert e.value.detail == "Forbidden"
    assert authz_request_forbidden_patcher["auth_request_mock"].called

    if not config.DEBUG and config_debug_save:
        config.DEBUG = config_debug_save
