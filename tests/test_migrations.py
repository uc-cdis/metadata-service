import json
import pytest
import os

from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from mds.config import DB_DSN, DEFAULT_AUTHZ_STR


class MigrationRunner:
    def __init__(self):
        self.action: str = ""
        self.target: str = ""
        current_dir = os.path.dirname(os.path.realpath(__file__))
        self.alembic_ini_path = os.path.join(current_dir, "../alembic.ini")

    async def upgrade(self, target: str):
        self.action = "upgrade"
        self.target = target
        await self._run_alembic_command()

    async def downgrade(self, target: str):
        self.action = "downgrade"
        self.target = target
        await self._run_alembic_command()

    async def _run_alembic_command(self):
        """
        Args:
            action (str): "upgrade" or "downgrade"
            target (str): "base", "head" or revision ID
        """

        def _run_command(connection):
            alembic_cfg = Config(self.alembic_ini_path)
            alembic_cfg.attributes["connection"] = connection
            if self.action == "upgrade":
                command.upgrade(alembic_cfg, self.target)
            elif self.action == "downgrade":
                command.downgrade(alembic_cfg, self.target)
            else:
                raise Exception(f"Unknown MigrationRunner action '{self.action}'")

        engine = create_async_engine(
            DB_DSN.set(drivername="postgresql+asyncpg"), echo=True
        )
        async with engine.begin() as conn:
            await conn.run_sync(_run_command)
        await engine.dispose()


def escape(str):
    # escape single quotes for SQL statement
    return str.replace(":", "\\:").replace("'", "''")


async def reset_migrations():
    migration_runner = MigrationRunner()
    await migration_runner.upgrade("head")


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
    db_session, old_metadata: dict, new_metadata: dict, authz_data: dict
):
    """
    We can't create metadata by using the `client` fixture because of two reasons:
    1) Calling the API when the db is in the downgraded state will give
       UndefinedColumnErrors (metadata.authz) because the db and the data model are out of sync
    2) this issue: https://github.com/encode/starlette/issues/440
    so inserting directly into the DB instead.
    """
    # after "add_authz_column" migration
    migration_runner = MigrationRunner()
    await migration_runner.upgrade("4d93784a25e5")

    # data values
    fake_guid = "123456"

    # insert data
    sql_authz_data = escape(json.dumps(authz_data))
    if new_metadata is not None:
        sql_new_metadata = escape(json.dumps(new_metadata))
        insert_stmt = f"INSERT INTO metadata (guid, authz, data) VALUES ('{fake_guid}', '{sql_authz_data}', '{sql_new_metadata}')"
    else:
        insert_stmt = f"INSERT INTO metadata (guid, authz, data) VALUES ('{fake_guid}', '{sql_authz_data}', null)"
    await db_session.execute(text(insert_stmt))

    try:
        # check that the request data was inserted correctly
        fetch_stmt = f"SELECT * FROM metadata WHERE guid = '{fake_guid}'"
        data = list((await db_session.execute(text(fetch_stmt))).all())
        row = dict(data[0]._mapping)
        assert row == {"guid": fake_guid, "data": new_metadata, "authz": authz_data}

        # downgrade to before "add_authz_column" migration
        await migration_runner.downgrade("f96cb3b2c523")

        # check that the migration moved the `authz` data into the `data` column
        fetch_stmt = f"SELECT * FROM metadata WHERE guid = '{fake_guid}'"
        data = list((await db_session.execute(text(fetch_stmt))).all())
        assert len(data) == 1
        row = dict(data[0]._mapping)
        assert row == {"guid": fake_guid, "data": old_metadata}

    finally:
        delete_stmt = f"DELETE FROM metadata WHERE guid = '{fake_guid}'"
        await db_session.execute(text(delete_stmt))
        await db_session.commit()

    reset_migrations()


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
    db_session, old_metadata: dict, new_metadata: dict, authz_data: dict
):
    """
    We can't create metadata by using the `client` fixture because of two reasons:
    1) Calling the API when the db is in the downgraded state will give
       UndefinedColumnErrors (metadata.authz) because the db and the data model are out of sync
    2) this issue: https://github.com/encode/starlette/issues/440
    so inserting directly into the DB instead.
    """

    # before "add_authz_column" migration
    migration_runner = MigrationRunner()
    await migration_runner.downgrade("f96cb3b2c523")

    fake_guid = "7891011"
    # insert data
    sql_old_metadata = escape(json.dumps(old_metadata))
    insert_stmt = f"INSERT INTO metadata (guid, data) VALUES ('{fake_guid}', '{sql_old_metadata}')"
    await db_session.execute(text(insert_stmt))
    await db_session.commit()

    try:
        fetch_stmt = f"SELECT * FROM metadata WHERE guid = '{fake_guid}'"
        data = list((await db_session.execute(text(fetch_stmt))).all())
        row = dict(data[0]._mapping)
        assert row == {"guid": fake_guid, "data": old_metadata}

        # run "add_authz_column" migration
        await migration_runner.upgrade("4d93784a25e5")

        # check correct `authz` data from `data` column
        fetch_stmt = f"SELECT * FROM metadata WHERE guid = '{fake_guid}'"
        data = list((await db_session.execute(text(fetch_stmt))).all())
        assert len(data) == 1
        row = dict(data[0]._mapping)
        assert row == {"guid": fake_guid, "data": new_metadata, "authz": authz_data}

    finally:
        delete_stmt = f"DELETE FROM metadata WHERE guid = '{fake_guid}'"
        await db_session.execute(text(delete_stmt))
        await db_session.commit()

    reset_migrations()


@pytest.mark.asyncio
async def test_6819874e85b9_upgrade(db_session):
    """
    We can't create metadata by using the `client` fixture because of this issue:
    https://github.com/encode/starlette/issues/440
    so inserting directly into the DB instead.
    """

    # before "remove_deprecated_metadata" migration
    migration_runner = MigrationRunner()
    await migration_runner.downgrade("3354f2c466ec")

    fake_guid = "7891011"
    # "percent" and "colon" are some edge cases that was causing the migration to fail
    old_metadata = {
        "foo": "bar",
        "bizz": "buzz",
        "percent": "50% for",
        "colon": "14(10):1534-47",
        "_uploader_id": "uploader",
        "_filename": "hello.txt",
        "_bucket": "mybucket",
        "_file_extension": ".txt",
    }
    new_metadata = {
        "foo": "bar",
        "bizz": "buzz",
        "percent": "50% for",
        "colon": "14(10):1534-47",
    }
    authz_data = {"version": 0, "_resource_paths": ["/programs/DEV"]}

    # insert data
    sql_old_metadata = escape(json.dumps(old_metadata))
    sql_authz_data = escape(json.dumps(authz_data))
    insert_stmt = f"INSERT INTO metadata (guid, data, authz) VALUES ('{fake_guid}', '{sql_old_metadata}', '{sql_authz_data}')"
    await db_session.execute(text(insert_stmt))
    await db_session.commit()

    try:
        # check that the request data was inserted correctly
        fetch_stmt = (
            f"SELECT guid, data, authz FROM metadata WHERE guid = '{fake_guid}'"
        )
        data = list((await db_session.execute(text(fetch_stmt))).all())
        row = dict(data[0]._mapping)
        assert row == {"guid": fake_guid, "data": old_metadata, "authz": authz_data}

        # run "remove_deprecated_metadata" migration
        await migration_runner.upgrade("6819874e85b9")

        # check that the migration removed the deprecated keys
        fetch_stmt = (
            f"SELECT guid, data, authz FROM metadata WHERE guid = '{fake_guid}'"
        )
        data = list((await db_session.execute(text(fetch_stmt))).all())
        assert len(data) == 1
        row = dict(data[0]._mapping)
        assert row == {"guid": fake_guid, "data": new_metadata, "authz": authz_data}

    finally:
        delete_stmt = f"DELETE FROM metadata WHERE guid = '{fake_guid}'"
        await db_session.execute(text(delete_stmt))
        await db_session.commit()

    reset_migrations()
