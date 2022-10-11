import json

from alembic.config import main as alembic_main
import pytest

from mds.config import DB_DSN, DEFAULT_AUTHZ_STR
from mds.models import db


def escape(str):
    # escape single quotes for SQL statement
    return str.replace("'", "''")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "old_metadata, new_metadata, authz_data",
    [
        # standard resource_paths in `authz` columns should get migrated into `data` column
        (
            {"foo": "bar", "_resource_paths": ["/programs/DEV"]},
            {"foo": "bar"},
            {"version": 0, "_resource_paths": ["/programs/DEV"]},
        ),
        # same for rows with empty ({}) `data` columns and populated `authz` columns
        (
            {"_resource_paths": ["/programs/DEV"]},
            {},
            {"version": 0, "_resource_paths": ["/programs/DEV"]},
        ),
        # handle single quotes and 2 single quotes in resource_paths
        (
            {
                "foo": "bar",
                "_resource_paths": [
                    "/programs/DEV",
                    "/path/with/single'quote/",
                    "/path/with/2/single''quotes/",
                ],
            },
            {"foo": "bar"},
            {
                "version": 0,
                "_resource_paths": [
                    "/programs/DEV",
                    "/path/with/single'quote/",
                    "/path/with/2/single''quotes/",
                ],
            },
        ),
        # handle single quotes and 2 single quotes in data
        (
            {
                "foo": "single'quote",
                "bar": "2single''quotes",
                "_resource_paths": ["/programs/DEV"],
            },
            {"foo": "single'quote", "bar": "2single''quotes"},
            {
                "version": 0,
                "_resource_paths": ["/programs/DEV"],
            },
        ),
        # default authz _resource_paths (["/open"]) do not get migrated into `data` column
        ({"foo": "bar"}, {"foo": "bar"}, {"version": 0, "_resource_paths": ["/open"]}),
        # "/open" as part of a list (not a default list) would get retained
        (
            {
                "foo": "bar",
                "_resource_paths": ["/open", "/programs/DEV", "/programs/DEVB"],
            },
            {"foo": "bar"},
            {
                "version": 0,
                "_resource_paths": ["/open", "/programs/DEV", "/programs/DEVB"],
            },
        ),
        # handle rows with null `data` columns and populated `authz` columns
        (
            {"_resource_paths": ["/programs/DEV"]},
            None,
            {"version": 0, "_resource_paths": ["/programs/DEV"]},
        ),
    ],
)
async def test_4d93784a25e5_downgrade(
    old_metadata: dict, new_metadata: dict, authz_data: dict
):
    """
    We can't create metadata by using the `client` fixture because of two reasons:
    1) Calling the API when the db is in the downgraded state will give
       UndefinedColumnErrors (metadata.authz) because the db and the data model are out of sync
    2) this issue: https://github.com/encode/starlette/issues/440
    so inserting directly into the DB instead.
    """

    # after "add_authz_column" migration
    alembic_main(["--raiseerr", "upgrade", "4d93784a25e5"])

    # data values
    fake_guid = "123456"

    async with db.with_bind(DB_DSN):
        # insert data
        sql_authz_data = escape(json.dumps(authz_data))
        if new_metadata is not None:
            sql_new_metadata = escape(json.dumps(new_metadata))
            insert_stmt = f"INSERT INTO metadata(\"guid\", \"authz\", \"data\") VALUES ('{fake_guid}', '{sql_authz_data}', '{sql_new_metadata}')"
        else:
            insert_stmt = f'INSERT INTO metadata("guid", "authz", "data") VALUES (\'{fake_guid}\', \'{sql_authz_data}\', null)'
        await db.scalar(db.text(insert_stmt))

        try:
            # check that the request data was inserted correctly
            data = await db.all(
                db.text(f"SELECT * FROM metadata WHERE guid = '{fake_guid}'")
            )
            row = {k: v for k, v in data[0].items()}
            assert row == {"guid": fake_guid, "data": new_metadata, "authz": authz_data}

            # downgrade to before "add_authz_column" migration
            alembic_main(["--raiseerr", "downgrade", "f96cb3b2c523"])

            # check that the migration moved the `authz` data into the `data` column
            data = await db.all(
                db.text(f"SELECT * FROM metadata WHERE guid = '{fake_guid}'")
            )
            assert len(data) == 1
            row = {k: v for k, v in data[0].items()}
            assert row == {"guid": fake_guid, "data": old_metadata}

        finally:
            await db.all(db.text(f"DELETE FROM metadata WHERE guid = '{fake_guid}'"))
        _reset_migrations()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "old_metadata, new_metadata, authz_data",
    [
        # standard authz data should get migrated into `authz` column
        # with standard version number (0)
        (
            {"foo": "bar", "_resource_paths": ["/programs/DEV", "/programs/OTHERDEV"]},
            {"foo": "bar"},
            {"version": 0, "_resource_paths": ["/programs/DEV", "/programs/OTHERDEV"]},
        ),
        # same with metadata with only resource_paths
        (
            {"_resource_paths": ["/programs/DEV", "/programs/OTHERDEV"]},
            {},
            {"version": 0, "_resource_paths": ["/programs/DEV", "/programs/OTHERDEV"]},
        ),
        # handle single quotes and 2 single quotes in resource_paths
        (
            {
                "foo": "bar",
                "_resource_paths": [
                    "/programs/DEV",
                    "/path/with/single'quote/",
                    "/path/with/2/single''quotes/",
                ],
            },
            {"foo": "bar"},
            {
                "version": 0,
                "_resource_paths": [
                    "/programs/DEV",
                    "/path/with/single'quote/",
                    "/path/with/2/single''quotes/",
                ],
            },
        ),
        # missing authz should get migrated to default value (["/open"]) in `authz` column
        ({"foo": "bar"}, {"foo": "bar"}, json.loads(DEFAULT_AUTHZ_STR)),
        # same with empty `data` column
        ({}, {}, {"version": 0, "_resource_paths": ["/open"]}),
        # some internal fields of metadata will get scrubbed on migration
        (
            {
                "foo": "bar",
                "_resource_paths": ["/programs/DEV", "/programs/OTHERDEV"],
                "_uploader_id": "uploader",
                "_filename": "hello.txt",
                "_bucket": "mybucket",
                "_file_extension": ".txt",
            },
            {"foo": "bar"},
            {"version": 0, "_resource_paths": ["/programs/DEV", "/programs/OTHERDEV"]},
        ),
    ],
)
async def test_4d93784a25e5_upgrade(
    old_metadata: dict, new_metadata: dict, authz_data: dict
):
    """
    We can't create metadata by using the `client` fixture because of two reasons:
    1) Calling the API when the db is in the downgraded state will give
       UndefinedColumnErrors (metadata.authz) because the db and the data model are out of sync
    2) this issue: https://github.com/encode/starlette/issues/440
    so inserting directly into the DB instead.
    """

    # before "add_authz_column" migration
    alembic_main(["--raiseerr", "downgrade", "f96cb3b2c523"])

    fake_guid = "7891011"

    async with db.with_bind(DB_DSN):

        # insert data
        sql_old_metadata = escape(json.dumps(old_metadata))
        insert_stmt = f"INSERT INTO metadata(\"guid\", \"data\") VALUES ('{fake_guid}', '{sql_old_metadata}')"
        await db.scalar(db.text(insert_stmt))

        try:
            # check that the request data was inserted correctly
            data = await db.all(
                db.text(f"SELECT * FROM metadata WHERE guid = '{fake_guid}'")
            )
            row = {k: v for k, v in data[0].items()}
            assert row == {"guid": fake_guid, "data": old_metadata}

            # run "add_authz_column" migration
            alembic_main(["--raiseerr", "upgrade", "4d93784a25e5"])

            # check that the migration created correct `authz` data from the `data` column
            data = await db.all(
                db.text(f"SELECT * FROM metadata WHERE guid = '{fake_guid}'")
            )
            assert len(data) == 1
            row = {k: v for k, v in data[0].items()}
            assert row == {"guid": fake_guid, "data": new_metadata, "authz": authz_data}

        finally:
            await db.all(db.text(f"DELETE FROM metadata WHERE guid = '{fake_guid}'"))

        _reset_migrations()


@pytest.mark.asyncio
async def test_6819874e85b9_upgrade():
    """
    We can't create metadata by using the `client` fixture because of this issue:
    https://github.com/encode/starlette/issues/440
    so inserting directly into the DB instead.
    """

    # before "remove_deprecated_metadata" migration
    alembic_main(["--raiseerr", "downgrade", "3354f2c466ec"])

    fake_guid = "7891011"
    old_metadata = {
        "foo": "bar",
        "bizz": "buzz",
        "_uploader_id": "uploader",
        "_filename": "hello.txt",
        "_bucket": "mybucket",
        "_file_extension": ".txt",
    }
    new_metadata = {"foo": "bar", "bizz": "buzz"}
    authz_data = {"version": 0, "_resource_paths": ["/programs/DEV"]}

    async with db.with_bind(DB_DSN):

        # insert data
        sql_old_metadata = escape(json.dumps(old_metadata))
        sql_authz_data = escape(json.dumps(authz_data))
        insert_stmt = f"INSERT INTO metadata(\"guid\", \"data\", \"authz\") VALUES ('{fake_guid}', '{sql_old_metadata}', '{sql_authz_data}')"
        await db.scalar(db.text(insert_stmt))

        try:
            # check that the request data was inserted correctly
            data = await db.all(
                db.text(
                    f"SELECT guid, data, authz FROM metadata WHERE guid = '{fake_guid}'"
                )
            )
            row = {k: v for k, v in data[0].items()}
            assert row == {"guid": fake_guid, "data": old_metadata, "authz": authz_data}

            # run "remove_deprecated_metadata" migration
            alembic_main(["--raiseerr", "upgrade", "6819874e85b9"])

            # check that the migration removed the deprecated keys
            data = await db.all(
                db.text(
                    f"SELECT guid, data, authz FROM metadata WHERE guid = '{fake_guid}'"
                )
            )
            assert len(data) == 1
            row = {k: v for k, v in data[0].items()}
            assert row == {"guid": fake_guid, "data": new_metadata, "authz": authz_data}

        finally:
            await db.all(db.text(f"DELETE FROM metadata WHERE guid = '{fake_guid}'"))

        _reset_migrations()


def _reset_migrations():
    alembic_main(["--raiseerr", "upgrade", "head"])
