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
    params=[
        "dg.TEST/87fced8d-b9c8-44b5-946e-c465c8f8f3d6",
        "87fced8d-b9c8-44b5-946e-c465c8f8f3d6",
    ]
)
def guid_mock(request):
    """
    Yields guid mock.
    """
    yield request.param


@pytest.fixture()
def signed_url_mock():
    """
    Yields signed url mock.
    """
    yield "https://mock-signed-url"


@pytest.fixture()
def download_endpoints(guid_mock):
    """
    Yields dictionary with guid_mock and download endpoints for mds and data
    access service.
    """
    yield {
        "guid_mock": guid_mock,
        "mds": f"/objects/{guid_mock}/download",
        "data_access": f"{config.DATA_ACCESS_SERVICE_ENDPOINT}/data/download/{guid_mock}",
    }


@pytest.fixture(
    params=[
        {
            "oldest": "934a05a1-993c-4ae0-bfd9-ae9239bba21d",
            "latest": "2c9975f7-ec98-42fc-a703-a2ce88b1dbcf",
        },
        {
            "oldest": "dg.TEST/fd851a55-3d0b-485d-b1c9-66acb8791de7",
            "latest": "dg.TEST/19010a4e-53f9-4a27-b69f-26fc73089a52",
        },
    ]
)
def guid_pair_mock(request):
    """"""
    yield request.param


@pytest.fixture()
def latest_setup(client, guid_pair_mock):
    non_mds_guid = "3507f4e5-e6f7-4ffa-b9fa-c82d0c16de91"
    setup = {
        "mds_latest_endpoint_with_oldest_guid": f"/objects/{guid_pair_mock['oldest']}/latest",
        "mds_latest_endpoint_with_non_mds_guid": f"/objects/{non_mds_guid}/latest",
        "indexd_latest_endpoint_with_oldest_guid": f"{config.INDEXING_SERVICE_ENDPOINT}/index/{guid_pair_mock['oldest']}/latest",
        "indexd_latest_endpoint_with_non_mds_guid": f"{config.INDEXING_SERVICE_ENDPOINT}/index/{non_mds_guid}/latest",
        "indexd_oldest_record_data": {
            "did": guid_pair_mock["oldest"],
            "size": 314,
        },
        "indexd_latest_record_data": {
            "did": guid_pair_mock["latest"],
            "size": 42,
        },
        "indexd_non_mds_record_data": {
            "did": non_mds_guid,
            "size": 271,
        },
        "mds_objects": {
            "oldest": {"guid": guid_pair_mock["oldest"], "data": dict(a=1, b=2)},
            "latest": {"guid": guid_pair_mock["latest"], "data": dict(a=3, b=4)},
        },
    }

    for mds_object in setup["mds_objects"].values():
        client.post(
            "/metadata/" + mds_object["guid"], json=mds_object["data"]
        ).raise_for_status()

    yield setup

    for mds_object in setup["mds_objects"].values():
        client.delete("/metadata/" + mds_object["guid"]).raise_for_status()


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
