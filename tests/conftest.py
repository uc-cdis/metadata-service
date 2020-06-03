import importlib

import pytest
from alembic.config import main
from starlette.config import environ
from starlette.testclient import TestClient

environ["TESTING"] = "TRUE"


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
