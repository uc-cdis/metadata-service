from cgi import test
from hashlib import new
from alembic.config import main as alembic_main
import pytest
import json

from mds.config import DB_DSN, DEFAULT_AUTHZ_STR
from mds.models import db


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "old_metadata, new_metadata, authz_data",
    [
        # standard resource_paths should get migrated into `authz` column with standard version number (0)
        (
            {"foo": "bar", "_resource_paths": ["/programs/DEV"]},
            '{"foo": "bar"}',
            '{"version": 0, "_resource_paths": ["/programs/DEV"]}',
        ),
        # default authz resource_paths do not get migrated into `data` column.
        ({"foo": "bar"}, '{"foo": "bar"}', DEFAULT_AUTHZ_STR),
        # handle null `data` columns (with populated `authz` columns)
        (
            {"_resource_paths": ["/programs/DEV"]},
            None,
            '{"version": 0, "_resource_paths": ["/programs/DEV"]}',
        ),
    ],
)
async def test_4d93784a25e5_downgrade(
    old_metadata: dict, new_metadata: str, authz_data: str
):
    """
    We can't create metadata by using the `client` fixture because of two reasons:
    1) the `authz` non-nullable column will not exist in the metadata table after the alembic downgrade
    2) this issue: https://github.com/encode/starlette/issues/440
    so inserting directly into the DB instead.
    """

    # after "add_authz_column" migration
    alembic_main(["--raiseerr", "upgrade", "4d93784a25e5"])

    # data values
    fake_guid = "7891011"

    async with db.with_bind(DB_DSN):
        # insert data
        if new_metadata is not None:
            insert_stmt = f"INSERT INTO metadata(\"guid\", \"authz\", \"data\") VALUES ('{fake_guid}', '{authz_data}', '{new_metadata}')"
        else:
            insert_stmt = f'INSERT INTO metadata("guid", "authz", "data") VALUES (\'{fake_guid}\', \'{authz_data}\', null)'
            # null will come back as 'None' in select statement
            new_metadata = "None"
        data = await db.scalar(db.text(insert_stmt))

        # check that the request data was inserted correctly
        data = await db.all(db.text(f"SELECT * FROM metadata"))
        request = {k: str(v) for row in data for k, v in row.items()}
        assert request["guid"] == fake_guid
        assert request["authz"] == authz_data.replace('"', "'")
        assert request["data"] == new_metadata.replace('"', "'")

        # downgrade to before "add_authz_column" migration
        alembic_main(["--raiseerr", "downgrade", "f96cb3b2c523"])

        # check that the migration moved the `authz` data into the `data` column
        data = await db.all(db.text("SELECT * FROM metadata"))
        request = {k: v for row in data for k, v in row.items()}
        assert request["guid"] == fake_guid
        assert "authz" not in request
        assert request["data"] == old_metadata

        await db.all(db.text(f"DELETE FROM metadata"))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "old_metadata, new_metadata, authz_data",
    [
        # standard authz data should get migrated into `authz` column with standard version number (0)
        (
            '{"foo": "bar", "_resource_paths": ["/programs/DEV"]}',
            {"foo": "bar"},
            {"version": 0, "_resource_paths": ["/programs/DEV"]},
        ),
        # missing authz should get migrated to default value (["/open"]) in `authz` column
        ('{"foo": "bar"}', {"foo": "bar"}, json.loads(DEFAULT_AUTHZ_STR)),
        # some internal fields of metadata will get scrubbed on migration
        (
            '{"foo": "bar", "_resource_paths": ["/programs/DEV"], "_uploader_id": "uploader","_filename": "hello.txt", "_bucket": "mybucket", "_file_extension": ".txt"}',
            {"foo": "bar"},
            {"version": 0, "_resource_paths": ["/programs/DEV"]},
        ),
    ],
)
async def test_4d93784a25e5_add_authz_column(
    old_metadata: str, new_metadata: dict, authz_data: dict
):

    # before "add_authz_column" migration
    alembic_main(["--raiseerr", "downgrade", "f96cb3b2c523"])

    fake_guid = "123456"

    async with db.with_bind(DB_DSN):

        # insert data
        insert_stmt = f"INSERT INTO metadata(\"guid\", \"data\") VALUES ('{fake_guid}', '{old_metadata}')"
        await db.scalar(db.text(insert_stmt))

        # check that the request data was inserted correctly
        data = await db.all(db.text("SELECT * FROM metadata"))
        request = {k: str(v) for row in data for k, v in row.items()}
        assert request["guid"] == fake_guid
        assert "authz" not in request
        assert json.loads(request["data"].replace("'", '"')) == json.loads(old_metadata)

        # run "add_authz_column" migration
        alembic_main(["--raiseerr", "upgrade", "4d93784a25e5"])

        # check that the migration created correct `authz` data from the `data` column
        data = await db.all(db.text(f"SELECT * FROM metadata"))
        request = {k: v for row in data for k, v in row.items()}
        assert request["guid"] == fake_guid
        assert request["authz"] == authz_data
        assert request["data"] == new_metadata

        await db.all(db.text("DELETE FROM metadata"))
