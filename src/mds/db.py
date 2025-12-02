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
from typing import Any, AsyncGenerator

from sqlalchemy import select, text, or_
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.dialects.postgresql import insert

from . import config
from .models_new import Metadata, MetadataAlias
from cdislogging import get_logger

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
        result = await self.db_session.execute(
            select(MetadataAlias).where(MetadataAlias.alias == alias)
        )
        alias_record = result.scalar_one_or_none()
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
                .returning(Metadata.guid, Metadata.data, Metadata.authz, text("xmax"))
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
    # TODO

    # async def get_aliases_for_guid(self, guid: str) -> list[str]:
    #     """Get all aliases for a GUID."""
    #     pass

    # async def create_aliases(self, guid: str, aliases: list[str]) -> list[str]:
    #     """Create new aliases."""
    #     pass

    # async def update_aliases(self, guid: str, aliases: list[str], merge: bool = True) -> list[str]:
    #     """Update aliases."""
    #     pass

    # async def delete_alias(self, guid: str, alias: str) -> str | None:
    #     """Delete single alias."""
    #     pass

    # async def delete_all_aliases(self, guid: str) -> int:
    #     """Delete all aliases for GUID."""
    #     pass

    # =========================================================================
    # Batch and Index Operations
    # =========================================================================
    # TODO

    # async def batch_create_metadata(self, data_list: list, overwrite: bool = False) -> dict:
    #     """Batch upsert."""
    #     pass

    # async def list_metadata_indexes(self) -> list[dict]:
    #     """List custom indexes on data column."""
    #     pass

    # async def create_metadata_index(self, path: str) -> None:
    #     """Create index on JSON path."""
    #     pass

    # async def drop_metadata_index(self, path: str) -> None:
    #     """Drop index on JSON path."""
    #     pass


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
