import importlib

import pytest
from alembic.config import main
import httpx
import respx
from starlette.config import environ
from starlette.testclient import TestClient

from unittest.mock import MagicMock, patch

environ["TESTING"] = "TRUE"
from mds import config

# NOTE: AsyncMock is included in unittest.mock but ONLY in Python 3.8+
class AsyncMock(MagicMock):
    async def __call__(self, *args, **kwargs):
        return super(AsyncMock, self).__call__(*args, **kwargs)


@pytest.fixture(autouse=True, scope="session")
def setup_test_database():
    from mds import config

    main(["--raiseerr", "upgrade", "head"])

    yield

    importlib.reload(config)
    if not config.TEST_KEEP_DB:
        main(["--raiseerr", "downgrade", "base"])


@pytest.fixture()
def client():
    from mds import config
    from mds.main import get_app

    importlib.reload(config)

    with TestClient(get_app()) as client:
        yield client


@pytest.fixture(
    scope="function",
    params=[
        # guid w/ prefix
        {
            "mock_guid": "dg.TEST/87fced8d-b9c8-44b5-946e-c465c8f8f3d6",
            "mock_signed_url": "https://mock-signed-url",
        },
        # guid w/ no prefix
        {
            "mock_guid": "87fced8d-b9c8-44b5-946e-c465c8f8f3d6",
            "mock_signed_url": "https://mock-signed-url",
        },
    ],
)
def valid_upload_file_patcher(client, request):
    patches = []

    data_upload_mocked_reponse = {
        "guid": request.param.get("mock_guid"),
        "url": request.param.get("mock_signed_url"),
    }
    data_upload_mock = respx.post(
        config.DATA_ACCESS_SERVICE_ENDPOINT.rstrip("/") + f"/data/upload",
        status_code=200,
        content=data_upload_mocked_reponse,
        alias="data_upload",
    )
    data_upload_guid_mock = respx.get(
        config.DATA_ACCESS_SERVICE_ENDPOINT.rstrip("/")
        + f"/data/upload/{request.param.get('mock_guid')}",
        status_code=200,
        content=data_upload_mocked_reponse,
        alias="data_upload_guid",
    )

    create_aliases_mock = respx.post(
        config.INDEXING_SERVICE_ENDPOINT.rstrip("/")
        + f"/index/{request.param.get('mock_guid')}/aliases",
        status_code=200,
        alias="create_aliases",
    )

    access_token_mock = MagicMock()
    patches.append(patch("authutils.token.fastapi.access_token", access_token_mock))
    patches.append(patch("mds.objects.access_token", access_token_mock))

    async def get_access_token(*args, **kwargs):
        return {"sub": "1"}

    access_token_mock.return_value = get_access_token

    for patched_function in patches:
        patched_function.start()

    yield {
        "data_upload_mock": data_upload_mock,
        "data_upload_guid_mock": data_upload_guid_mock,
        "create_aliases_mock": create_aliases_mock,
        "access_token_mock": access_token_mock,
        "data_upload_mocked_reponse": data_upload_mocked_reponse,
    }

    client.delete(f"/metadata/{request.param.get('mock_guid')}")
    for patched_function in patches:
        patched_function.stop()


@pytest.fixture(
    scope="function",
    params=[
        {
            "mock_guid": "dg.TEST/87fced8d-b9c8-44b5-946e-c465c8f8f3d6",
            "mock_signed_url": "https://mock-signed-url",
        }
    ],
)
def no_authz_upload_file_patcher(client, request):
    """
    Same as valid_upload_file_patcher except /data/upload requests are mocked
    as returning a 403 for invalid authz
    """
    patches = []

    data_upload_mocked_reponse = {"guid": request.param.get("mock_guid")}
    data_upload_mock = respx.post(
        config.DATA_ACCESS_SERVICE_ENDPOINT.rstrip("/") + f"/data/upload",
        status_code=403,
        content=data_upload_mocked_reponse,
        alias="data_upload",
    )
    data_upload_guid_mock = respx.get(
        config.DATA_ACCESS_SERVICE_ENDPOINT.rstrip("/")
        + f"/data/upload/{request.param.get('mock_guid')}",
        status_code=403,
        content=data_upload_mocked_reponse,
        alias="data_upload_guid",
    )

    create_aliases_mock = respx.post(
        config.INDEXING_SERVICE_ENDPOINT.rstrip("/")
        + f"/index/{request.param.get('mock_guid')}/aliases",
        status_code=200,
        alias="create_aliases",
    )

    access_token_mock = MagicMock()
    patches.append(patch("authutils.token.fastapi.access_token", access_token_mock))
    patches.append(patch("mds.objects.access_token", access_token_mock))

    async def get_access_token(*args, **kwargs):
        return {"sub": "1"}

    access_token_mock.return_value = get_access_token

    for patched_function in patches:
        patched_function.start()

    yield {
        "data_upload_mock": data_upload_mock,
        "data_upload_guid_mock": data_upload_guid_mock,
        "create_aliases_mock": create_aliases_mock,
        "access_token_mock": access_token_mock,
        "data_upload_mocked_reponse": data_upload_mocked_reponse,
    }

    client.delete(f"/metadata/{request.param.get('mock_guid')}")
    for patched_function in patches:
        patched_function.stop()


@pytest.fixture(
    scope="function",
    params=[
        {
            "mock_guid": "dg.TEST/87fced8d-b9c8-44b5-946e-c465c8f8f3d6",
            "mock_signed_url": "https://mock-signed-url",
        }
    ],
)
def no_authz_create_aliases_patcher(client, request):
    """
    Same as valid_upload_file_patcher except /aliases requests are mocked
    as returning a 403 for invalid authz
    """
    patches = []

    data_upload_mocked_reponse = {
        "guid": request.param.get("mock_guid"),
        "url": request.param.get("mock_signed_url"),
    }
    data_upload_mock = respx.post(
        config.DATA_ACCESS_SERVICE_ENDPOINT.rstrip("/") + f"/data/upload",
        status_code=200,
        content=data_upload_mocked_reponse,
        alias="data_upload",
    )

    create_aliases_mock = respx.post(
        config.INDEXING_SERVICE_ENDPOINT.rstrip("/")
        + f"/index/{request.param.get('mock_guid')}/aliases",
        status_code=403,
        alias="create_aliases",
    )

    access_token_mock = MagicMock()
    patches.append(patch("authutils.token.fastapi.access_token", access_token_mock))
    patches.append(patch("mds.objects.access_token", access_token_mock))

    async def get_access_token(*args, **kwargs):
        return {"sub": "1"}

    access_token_mock.return_value = get_access_token

    for patched_function in patches:
        patched_function.start()

    yield {
        "data_upload_mock": data_upload_mock,
        "create_aliases_mock": create_aliases_mock,
        "access_token_mock": access_token_mock,
        "data_upload_mocked_reponse": data_upload_mocked_reponse,
    }

    client.delete(f"/metadata/{request.param.get('mock_guid')}")
    for patched_function in patches:
        patched_function.stop()


@pytest.fixture(
    scope="function",
    params=[
        # guid w/ prefix
        {
            "mock_guid": "dg.TEST/87fced8d-b9c8-44b5-946e-c465c8f8f3d6",
            "mock_signed_url": "https://mock-signed-url",
        }
    ],
)
def upload_failure_file_patcher(client, request):
    """
    Same as valid_upload_file_patcher except /data/upload requests are mocked
    as returning a 500
    """
    patches = []

    data_upload_mocked_reponse = {}
    data_upload_mock = respx.post(
        config.DATA_ACCESS_SERVICE_ENDPOINT.rstrip("/") + f"/data/upload",
        status_code=500,
        content=data_upload_mocked_reponse,
        alias="data_upload",
    )

    create_aliases_mock = respx.post(
        config.INDEXING_SERVICE_ENDPOINT.rstrip("/")
        + f"/index/{request.param.get('mock_guid')}/aliases",
        status_code=200,
        alias="create_aliases",
    )

    access_token_mock = MagicMock()
    patches.append(patch("authutils.token.fastapi.access_token", access_token_mock))
    patches.append(patch("mds.objects.access_token", access_token_mock))

    async def get_access_token(*args, **kwargs):
        return {"sub": "1"}

    access_token_mock.return_value = get_access_token

    for patched_function in patches:
        patched_function.start()

    yield {
        "data_upload_mock": data_upload_mock,
        "create_aliases_mock": create_aliases_mock,
        "access_token_mock": access_token_mock,
        "data_upload_mocked_reponse": data_upload_mocked_reponse,
    }

    client.delete(f"/metadata/{request.param.get('mock_guid')}")
    for patched_function in patches:
        patched_function.stop()


@pytest.fixture(
    scope="function",
    params=[
        # guid w/ prefix
        {
            "mock_guid": "dg.TEST/87fced8d-b9c8-44b5-946e-c465c8f8f3d6",
            "mock_signed_url": "https://mock-signed-url",
        }
    ],
)
def create_aliases_failure_patcher(client, request):
    """
    Same as valid_upload_file_patcher except /aliases requests are mocked
    as returning a 500
    """
    patches = []

    data_upload_mocked_reponse = {
        "guid": request.param.get("mock_guid"),
        "url": request.param.get("mock_signed_url"),
    }
    data_upload_mock = respx.post(
        config.DATA_ACCESS_SERVICE_ENDPOINT.rstrip("/") + f"/data/upload",
        status_code=200,
        content=data_upload_mocked_reponse,
        alias="data_upload",
    )

    create_aliases_mock = respx.post(
        config.INDEXING_SERVICE_ENDPOINT.rstrip("/")
        + f"/index/{request.param.get('mock_guid')}/aliases",
        status_code=500,
        alias="create_aliases",
    )

    access_token_mock = MagicMock()
    patches.append(patch("authutils.token.fastapi.access_token", access_token_mock))
    patches.append(patch("mds.objects.access_token", access_token_mock))

    async def get_access_token(*args, **kwargs):
        return {"sub": "1"}

    access_token_mock.return_value = get_access_token

    for patched_function in patches:
        patched_function.start()

    yield {
        "data_upload_mock": data_upload_mock,
        "create_aliases_mock": create_aliases_mock,
        "access_token_mock": access_token_mock,
        "data_upload_mocked_reponse": data_upload_mocked_reponse,
    }

    client.delete(f"/metadata/{request.param.get('mock_guid')}")
    for patched_function in patches:
        patched_function.stop()


@pytest.fixture(
    scope="function",
    params=[
        # guid w/ prefix
        {
            "mock_guid": "dg.TEST/87fced8d-b9c8-44b5-946e-c465c8f8f3d6",
            "mock_signed_url": "https://mock-signed-url",
        }
    ],
)
def create_aliases_duplicate_patcher(client, request):
    """
    Same as valid_upload_file_patcher except /aliases requests are mocked
    as returning a 500
    """
    patches = []

    data_upload_mocked_reponse = {
        "guid": request.param.get("mock_guid"),
        "url": request.param.get("mock_signed_url"),
    }
    data_upload_mock = respx.post(
        config.DATA_ACCESS_SERVICE_ENDPOINT.rstrip("/") + f"/data/upload",
        status_code=200,
        content=data_upload_mocked_reponse,
        alias="data_upload",
    )

    create_aliases_mock = respx.post(
        config.INDEXING_SERVICE_ENDPOINT.rstrip("/")
        + f"/index/{request.param.get('mock_guid')}/aliases",
        status_code=409,
        alias="create_aliases",
    )

    access_token_mock = MagicMock()
    patches.append(patch("authutils.token.fastapi.access_token", access_token_mock))
    patches.append(patch("mds.objects.access_token", access_token_mock))

    async def get_access_token(*args, **kwargs):
        return {"sub": "1"}

    access_token_mock.return_value = get_access_token

    for patched_function in patches:
        patched_function.start()

    yield {
        "data_upload_mock": data_upload_mock,
        "create_aliases_mock": create_aliases_mock,
        "access_token_mock": access_token_mock,
        "data_upload_mocked_reponse": data_upload_mocked_reponse,
    }

    client.delete(f"/metadata/{request.param.get('mock_guid')}")
    for patched_function in patches:
        patched_function.stop()
