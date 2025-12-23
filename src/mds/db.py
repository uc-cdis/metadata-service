"""
WIP
This file houses the database logic.
For schema/model of particular tables, go to `models.py`

OVERVIEW
--------

We're using SQLAlchemy's async support alongside FastAPI's dependency injection.

This file contains the logic for database manipulation in a "data access layer"
class, such that other areas of the code have simple method calls which
won't require knowledge on how to manage the session or interact with the db.
The session will be managed by dep injection of FastAPI's endpoints.
The logic that sets up the sessions is in this file.


DETAILS
-------
What do we do in this file?

- We create a sqlalchemy engine and session maker factory as globals
    - This reads in the db URL from config
- We define a data access layer class here which isolates the database manipulations
    - All CRUD operations go through this interface instead of bleeding specific database
      manipulations into the higher level web app endpoint code
- We create a function which yields an instance of the data access layer class with
  a fresh session from the session maker factory
    - This is what gets injected into endpoint code using FastAPI's dep injections
"""
import hashlib
import re

from typing import Any, AsyncGenerator

from sqlalchemy import delete, select, text, or_, literal_column
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.dialects.postgresql import insert
from fastapi import Request

from . import config
from .models import Metadata, MetadataAlias
from cdislogging import get_logger

INDEX_REGEXP = re.compile(r"data #>> '{(.+)}'::text")

logger = get_logger(__name__)

engine: AsyncEngine | None = None
async_sessionmaker_instance: async_sessionmaker | None = None


async def initiate_db() -> None:
    """
    Initialize the database engine and async sessionmaker.
    """
    global engine, async_sessionmaker_instance

    db_dsn = config.DB_DSN
    # set asyncpg for async support
    async_dsn = db_dsn.set(drivername="postgresql+asyncpg")

    logger.info(f"Initializing database with DSN: {async_dsn}")

    engine = create_async_engine(
        url=async_dsn,
        pool_size=config.DB_POOL_MIN_SIZE,
        max_overflow=config.DB_POOL_MAX_SIZE - config.DB_POOL_MIN_SIZE,
        echo=config.DB_ECHO,
        connect_args={"ssl": config.DB_SSL} if config.DB_SSL else {},
        pool_pre_ping=True,
    )

    # creates AsyncSession instances
    async_sessionmaker_instance = async_sessionmaker(
        bind=engine, expire_on_commit=False
    )


def get_db_engine_and_sessionmaker() -> tuple[AsyncEngine, async_sessionmaker]:
    """
    Get the db engine and sessionmaker instances.
    """
    global engine, async_sessionmaker_instance
    if engine is None or async_sessionmaker_instance is None:
        raise Exception("Database not initialized. Call initiate_db() first.")
    return engine, async_sessionmaker_instance


async def get_session(request: Request):
    async_sessionmaker = request.app.state.async_sessionmaker
    async with async_sessionmaker() as session:
        yield session


class DataAccessLayer:
    """
    Defines an abstract interface to manipulate the database. Instances are given a session to
    act within.
    """

    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session

    async def test_connection(self) -> None:
        """
        Ensure we can actually communicate with the db.
        """
        await self.db_session.execute(text("SELECT 1;"))

    async def get_current_time(self):
        """
        Get the current database timestamp. Used for status endpoint.
        """
        result = await self.db_session.execute(text("SELECT now();"))
        return result.scalar()

    # =========================================================================
    # Metadata CRUD Operations
    # =========================================================================

    async def get_metadata(self, guid: str) -> dict | None:
        """
        Get single metadata by GUID.

        Args:
            guid: The GUID to look up

        Returns:
            Dictionary with guid, data, and authz keys, or None if not found
        """
        result = await self.db_session.execute(
            select(Metadata).where(Metadata.guid == guid)
        )
        metadata = result.scalar_one_or_none()
        if metadata:
            return metadata.to_dict()
        return None

    async def get_metadata_by_alias(self, alias: str) -> dict | None:
        """
        Resolve alias to metadata.

        Args:
            alias: The alias to look up

        Returns:
            Dictionary with guid, data, and authz keys, or None if alias not found
        """
        alias_record = await self.get_alias(alias)
        if alias_record:
            return await self.get_metadata(alias_record.guid)
        return None

    async def create_metadata(
        self, guid: str, data: dict | None, authz: dict, overwrite: bool = False
    ) -> tuple[dict, bool]:
        """
        Insert new metadata. Optionally overwrites existing record if overwrite=True.

        Args:
            guid: The GUID for the new metadata
            data: The metadata content (can be None)
            authz: Authorization info (required)
            overwrite: If True, update existing record on conflict

        Returns:
            Tuple of (metadata dict, was_created boolean)
            - was_created is True if a new record was inserted
            - was_created is False if an existing record was updated (when overwrite=True)

        Raises:
            IntegrityError: If guid already exists and overwrite=False
        """
        if overwrite:
            stmt = (
                insert(Metadata)
                .values(guid=guid, data=data, authz=authz)
                .on_conflict_do_update(
                    index_elements=[Metadata.guid],
                    set_={"data": data},
                )
                .returning(
                    Metadata.guid, Metadata.data, Metadata.authz, literal_column("xmax")
                )
            )
            result = await self.db_session.execute(stmt)
            row = result.one()
            # xmax = 0 means new row was inserted, xmax != 0 means row was updated
            was_created = row.xmax == 0
            return {"guid": row.guid, "data": row.data, "authz": row.authz}, was_created
        else:
            # Simple insert will raise IntegrityError on conflict
            # let the exception bubble up like audit-service
            metadata = Metadata(guid=guid, data=data, authz=authz)
            self.db_session.add(metadata)
            await self.db_session.flush()
            return metadata.to_dict(), True

    async def update_metadata(
        self, guid: str, data: dict, merge: bool = False
    ) -> dict | None:
        """
        Update existing metadata.

        If `merge` is True, then any top-level keys that are not in the new data will be
        kept, and those that also exist in the new data will be replaced completely. This
        is also known as the shallow merge. The metadata service currently doesn't support
        deep merge.

        Args:
            guid: The GUID of the record to update
            data: New metadata content
            merge: If True, merge with existing data (shallow merge at top-level keys).
                   If False, replace existing data entirely.

        Returns:
            Updated metadata dict, or None if GUID not found
        """
        result = await self.db_session.execute(
            select(Metadata).where(Metadata.guid == guid)
        )
        metadata = result.scalar_one_or_none()

        if not metadata:
            return None

        if merge and metadata.data:
            merged_data = {**metadata.data, **data}
            metadata.data = merged_data
        else:
            metadata.data = data

        await self.db_session.flush()
        return metadata.to_dict()

    async def delete_metadata(self, guid: str) -> dict | None:
        """
        Delete metadata by GUID.

        Args:
            guid: The GUID to delete

        Returns:
            Deleted metadata dict, or None if GUID not found
        """
        result = await self.db_session.execute(
            select(Metadata).where(Metadata.guid == guid)
        )
        metadata = result.scalar_one_or_none()

        if not metadata:
            return None

        deleted_data = metadata.to_dict()
        await self.db_session.delete(metadata)
        await self.db_session.flush()
        return deleted_data

    async def search_metadata(
        self,
        filters: dict[str, list[str]],
        limit: int = 10,
        offset: int = 0,
        return_data: bool = False,
    ) -> dict[str, dict] | list[str]:
        """
        Search metadata with filters.

        Args:
            filters: Dict of path -> list of values to match.
                     Path can use dot notation for nested keys, e.g. "a.b.c".
            limit: Maximum number of records to return
            offset: Number of records to skip
            return_data: If True, return dict of guid -> data.
                         If False, return list of guids only.

        Returns:
            Either dict[guid, data] or list[guid] depending on return_data flag
        """
        # When return_data=False, only fetch guids
        if return_data:
            query = select(Metadata.guid, Metadata.data)
        else:
            query = select(Metadata.guid)

        # Apply filters
        for path, values in filters.items():
            if "*" in values:
                path_parts = path.split(".")
                field = path_parts.pop()
                if path_parts:
                    query = query.where(Metadata.data[path_parts].has_key(field))
                else:
                    query = query.where(Metadata.data.has_key(field))
            else:
                values = ["*" if v == "\\*" else v for v in values]
                path_parts = path.split(".") if "." in path else path

                conditions = []
                for v in values:
                    conditions.append(Metadata.data[path_parts].astext == v)

                if conditions:
                    query = query.where(or_(*conditions))

        # TODO/FIXME: There's no updated date on the records, and without that
        # this "pagination" is prone to produce inconsistent results if someone is
        # trying to paginate using offset WHILE data is being added
        #
        # The only real way to try and reduce that risk
        # is to order by updated date (so newly added stuff is
        # at the end and new records don't end up in a page earlier on)
        # This is how our indexing service handles this situation.
        #
        # But until we have an updated_date, we can't do that, so naively order by
        # GUID for now and accept this inconsistency risk.
        query = query.order_by(Metadata.guid)
        query = query.offset(offset).limit(limit)

        result = await self.db_session.execute(query)
        rows = result.all()

        if return_data:
            return {row.guid: row.data for row in rows}
        else:
            return [row.guid for row in rows]

    # =========================================================================
    # Alias Operations
    # =========================================================================

    async def get_alias(self, alias: str) -> MetadataAlias | None:
        """
        Get alias

        Args:
            alias: The alias to look up

        Returns:
            MetadataAlias object or None if alias not found
        """
        result = await self.db_session.execute(
            select(MetadataAlias).where(MetadataAlias.alias == alias)
        )
        return result.scalar_one_or_none()

    async def get_aliases_for_guid(self, guid: str) -> list[str]:
        """
        Get all aliases for a GUID.

        Args:
            guid: The GUID to look up aliases for

        Returns:
            List of alias strings
        """
        result = await self.db_session.execute(
            select(MetadataAlias.alias).where(MetadataAlias.guid == guid)
        )
        rows = result.scalars().all()
        return list(rows)

    async def create_aliases(self, guid: str, aliases: list[str]) -> list[str]:
        """
        Create new metadata aliases for a GUID.

        Args:
            guid: The GUID to create aliases for
            aliases: List of alias strings to create

        Returns:
            List of created alias strings

        Raises:
            IntegrityError: If GUID doesn't exist (ForeignKeyViolation) or
                           alias already exists (UniqueViolation)
        """
        unique_aliases = list(dict.fromkeys(aliases))

        for alias in unique_aliases:
            logger.debug(f"inserting MetadataAlias(alias={alias}, guid={guid})")
            alias_record = MetadataAlias(alias=alias, guid=guid)
            self.db_session.add(alias_record)

        await self.db_session.flush()
        return unique_aliases

    async def update_aliases(
        self, guid: str, aliases: list[str], merge: bool = False
    ) -> list[str]:
        """
        Update metadata aliases for a GUID.

        If `merge` is True, existing aliases not in the new list are kept.
        If `merge` is False (default), existing aliases not in the new list are deleted.

        Args:
            guid: The GUID to update aliases for
            aliases: New list of alias strings
            merge: If True, keep existing aliases not in new list.
                   If False, remove existing aliases not in new list.

        Returns:
            Final list of alias strings after update

        Raises:
            IntegrityError: If GUID doesn't exist (asyncpg ForeignKeyViolation) or
                           alias already belongs to different GUID (asyncpg UniqueViolation).
                           Sqlalchemy IntegrityError wraps both
        """
        requested_aliases = set(aliases)
        logger.debug(f"requested_aliases: {requested_aliases}")

        result = await self.db_session.execute(
            select(MetadataAlias).where(MetadataAlias.guid == guid)
        )
        existing_alias_records = result.scalars().all()
        existing_aliases = {record.alias for record in existing_alias_records}
        aliases_to_add = requested_aliases - existing_aliases

        # If not merging, delete aliases that are not in requested set
        if not merge:
            for record in existing_alias_records:
                if record.alias not in requested_aliases:
                    logger.debug(
                        f"deleting MetadataAlias(alias={record.alias}, guid={guid})"
                    )
                    await self.db_session.delete(record)
            final_aliases = requested_aliases
        else:
            final_aliases = requested_aliases | existing_aliases

        # Add new aliases
        for alias in aliases_to_add:
            logger.debug(f"inserting MetadataAlias(alias={alias}, guid={guid})")
            alias_record = MetadataAlias(alias=alias, guid=guid)
            self.db_session.add(alias_record)

        await self.db_session.flush()
        return sorted(list(final_aliases))

    async def delete_alias(self, guid: str, alias: str) -> dict | None:
        """
        Delete a single metadata alias for a GUID.

        Args:
            guid: The GUID the alias belongs to
            alias: The alias to delete

        Returns:
            Dictionary with alias and guid if deleted, None if not found
        """
        result = await self.db_session.execute(
            select(MetadataAlias).where(
                MetadataAlias.guid == guid, MetadataAlias.alias == alias
            )
        )
        alias_record = result.scalar_one_or_none()

        if not alias_record:
            return None

        deleted_data = alias_record.to_dict()
        logger.debug(f"deleting MetadataAlias(alias={alias}, guid={guid})")
        await self.db_session.delete(alias_record)
        await self.db_session.flush()
        return deleted_data

    async def delete_all_aliases(self, guid: str) -> int:
        """
        Delete all aliases for a GUID.

        Issues a bulk DELETE without selecting rows first, so it cannot log the aliases removed.
        Loading them would require a SELECT and negate the performance benefit.

        Args:
            guid: The GUID to delete all aliases for

        Returns:
            Number of aliases deleted
        """
        result = await self.db_session.execute(
            delete(MetadataAlias).where(MetadataAlias.guid == guid)
        )
        return result.rowcount

    # =========================================================================
    # Batch and Index Operations
    # =========================================================================

    async def batch_create_metadata(
        self,
        data_list: list[dict],
        overwrite: bool = False,
        default_authz: dict | None = None,
        forbidden_ids: set[str] | None = None,
    ) -> dict[str, list[str]]:
        """
        Batch create/update metadata records.

        TODO: evaluate efficiency compared to old GINO implementation
        The old GINO implementation explicitly cached the statement in postgres
        This current implentation depends on implicit sqlalchemy caching, and asyncpg caching prepared statments
        We should definitely do batch creation benchmarking

        Additional changes from previous GINO implmentation:
        - Old: `overwrite=True` by default
        - New: `overwrite=False` by default (here at DAL layer)

        Args:
            data_list: List of dicts with 'guid' and 'data' keys
            overwrite: If True, update existing records on conflict.
                       If False, skip existing records (report as conflict).
            default_authz: Default authz value to use for new records.
            forbidden_ids: Set of GUIDs that are not allowed (e.g., 'upload').

        Returns:
            Dict with keys: 'created', 'updated', 'conflict', 'bad_input'
            Each value is a list of GUIDs in that category.
        """
        created = []
        updated = []
        conflict = []
        bad_input = []

        if forbidden_ids is None:
            forbidden_ids = set()

        if default_authz is None:
            default_authz = {}

        for item in data_list:
            guid = item.get("guid")
            data = item.get("data")

            if guid in forbidden_ids:
                bad_input.append(guid)
                continue

            if overwrite:
                stmt = (
                    insert(Metadata)
                    .values(guid=guid, data=data, authz=default_authz)
                    .on_conflict_do_update(
                        index_elements=[Metadata.guid],
                        set_={"data": data},
                    )
                    .returning(literal_column("xmax"))
                )
                result = await self.db_session.execute(stmt)
                row = result.one()
                if row.xmax == 0:
                    created.append(guid)
                else:
                    updated.append(guid)
            else:
                # Use a savepoint so we can catch duplicates without
                # rolling back the entire transaction
                async with self.db_session.begin_nested():
                    try:
                        metadata = Metadata(guid=guid, data=data, authz=default_authz)
                        self.db_session.add(metadata)
                        await self.db_session.flush()
                        created.append(guid)
                    except IntegrityError as e:
                        # Check for unique violation (duplicate key)
                        if "duplicate key" in str(e).lower():
                            conflict.append(guid)
                        else:
                            raise

        return {
            "created": created,
            "updated": updated,
            "conflict": conflict,
            "bad_input": bad_input,
        }

    async def list_metadata_indexes(self) -> list[str]:
        """
        List all the metadata key paths indexed in the database.

        Returns:
            List of key paths
        """
        oid_result = await self.db_session.execute(
            text(
                """
                SELECT c.oid
                FROM pg_catalog.pg_class c
                LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
                WHERE pg_catalog.pg_table_is_visible(c.oid)
                AND c.relname = :table_name AND c.relkind in
                ('r', 'v', 'm', 'f', 'p')
                """
            ).bindparams(table_name=Metadata.__tablename__)
        )
        oid = oid_result.scalar()

        # return early if table doesn't exist
        if oid is None:
            return []

        indexes_result = await self.db_session.execute(
            text(
                """
                SELECT
                    i.relname as relname,
                    pg_get_expr(ix.indexprs, :table_oid) as indexprs
                FROM
                    pg_class t
                    join pg_index ix on t.oid = ix.indrelid
                    join pg_class i on i.oid = ix.indexrelid
                WHERE
                    t.relkind IN ('r', 'v', 'f', 'm', 'p')
                    and t.oid = :table_oid
                    and ix.indisprimary = 'f'
                ORDER BY
                    t.relname,
                    i.relname
                """
            ).bindparams(table_oid=oid)
        )
        indexes = indexes_result.all()

        rv = []
        for name, prs in indexes:
            if name.startswith("path_idx_"):
                matches = INDEX_REGEXP.findall(prs)
                if matches:
                    rv.append(".".join(matches[0].split(",")))
        return rv

    async def create_metadata_index(self, path: str) -> str:
        """
        Create a database index on the given metadata key path.

        Args:
            path: Key path

        Returns:
            The path that was indexed

        Raises:
            sqlalchemy.exc.ProgrammingError: If an index already exists for this path
                (wraps asyncpg.exceptions.DuplicateTableError)
        """
        import hashlib

        path = ",".join(path.split(".")).strip()
        name = hashlib.sha256(path.encode()).hexdigest()[:8]
        await self.db_session.execute(
            text(
                f"CREATE INDEX path_idx_{name} ON {Metadata.__tablename__} ((data #>> '{{%s}}'))"
                % path
            )
        )

        rv = ".".join(path.split(","))
        return rv

    async def drop_metadata_index(self, path: str) -> None:
        """
        Drop the database index on the given metadata key path.

        Args:
            path: Key path

        Raises:
            sqlalchemy.exc.ProgrammingError: If no index exists for this path
        """
        path = ",".join(path.split(".")).strip()
        name = hashlib.sha256(path.encode()).hexdigest()[:8]
        await self.db_session.execute(text(f"DROP INDEX path_idx_{name}"))


async def get_data_access_layer() -> AsyncGenerator[DataAccessLayer, Any]:
    """
    Create an AsyncSession and yield an instance of the Data Access Layer,
    which acts as an abstract interface to manipulate the database.

    Can be injected as a dependency in FastAPI endpoints.
    """
    global async_sessionmaker_instance
    if async_sessionmaker_instance is None:
        raise Exception("Database not initialized. Call initiate_db() first.")

    async with async_sessionmaker_instance() as session:
        async with session.begin():
            yield DataAccessLayer(session)
