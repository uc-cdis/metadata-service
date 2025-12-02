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
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from . import config
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
    # TODO

    # async def get_metadata(self, guid: str) -> dict | None:
    #     """Get single metadata by GUID."""
    #     pass

    # async def get_metadata_by_alias(self, alias: str) -> dict | None:
    #     """Resolve alias to metadata."""
    #     pass

    # async def create_metadata(self, guid: str, data: dict, authz: dict) -> dict:
    #     """Insert new metadata."""
    #     pass

    # async def update_metadata(self, guid: str, data: dict, merge: bool = True) -> dict | None:
    #     """Update existing metadata."""
    #     pass

    # async def delete_metadata(self, guid: str) -> dict | None:
    #     """Delete metadata by GUID."""
    #     pass

    # async def search_metadata(self, filters: dict, limit: int, offset: int, return_data: bool) -> list:
    #     """Query with filters."""
    #     pass

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
