import pytest
import pytest_asyncio
from datetime import datetime

from sqlalchemy.exc import IntegrityError, OperationalError, ProgrammingError

from mds.db import (
    initiate_db,
    get_db_engine_and_sessionmaker,
    DataAccessLayer,
)
from mds.models import Metadata, MetadataAlias, Base


# =============================================================================
# Test Fixtures and Helpers
# =============================================================================


@pytest_asyncio.fixture(scope="function")
async def data_access_layer():
    """
    Creates data_access_layer instance with a fresh db session.
    Use create_sample_data() helper if tests need pre-populated data.

    TODO: Consider not removing the indexes with the data_access_layer methods so as to be dependent here
    """
    initiate_db()
    _, session_maker = get_db_engine_and_sessionmaker()

    # Start with empty db (clear data and indexes)
    async with session_maker() as cleanup_session:
        # Clear all metadata
        await cleanup_session.execute(Metadata.__table__.delete())

        # Drop any custom path indexes
        cleanup_data_access_layer = DataAccessLayer(cleanup_session)
        existing_indexes = await cleanup_data_access_layer.list_metadata_indexes()
        for path in existing_indexes:
            await cleanup_data_access_layer.drop_metadata_index(path)

        await cleanup_session.commit()

    async with session_maker() as session:
        await session.begin()
        try:
            data_access_layer = DataAccessLayer(session)
            yield data_access_layer
        finally:
            await session.rollback()


async def create_sample_data(data_access_layer: DataAccessLayer) -> None:
    """
    Populate the database with sample metadata and aliases for testing.

    TODO: Consider inserting data directly via SQLAlchemy session instead of
    using data_access_layer methods, should be fine for now though
    """
    # Create sample metadata
    await data_access_layer.create_metadata(
        "sample_guid1", {"key1": "value1", "nested": {"a": "b"}}, {"authz": "public"}
    )
    await data_access_layer.create_metadata(
        "sample_guid2", {"key2": "value2"}, {"authz": "private"}
    )
    await data_access_layer.create_metadata(
        "sample_guid3", {"key1": "value1", "key3": "value3"}, {"authz": "public"}
    )

    # Create sample aliases
    await data_access_layer.create_aliases(
        "sample_guid1", ["sample_alias1", "sample_alias1a"]
    )
    await data_access_layer.create_aliases("sample_guid2", ["sample_alias2"])


# =============================================================================
# Database Infrastructure Tests
# =============================================================================


class TestDatabaseInfrastructure:
    """Tests for database initialization and connectivity."""

    @pytest.mark.asyncio
    async def test_initiate_db(self):
        """Verify engine and sessionmaker are created correctly."""
        initiate_db()
        eng, sessionmaker = get_db_engine_and_sessionmaker()
        assert eng is not None
        assert sessionmaker is not None

    @pytest.mark.asyncio
    async def test_connection(self, data_access_layer):
        """Verify database connectivity via data_access_layer."""
        from unittest.mock import AsyncMock

        # Success case
        result = await data_access_layer.test_connection()
        assert result is None

        # Failure case, make sure it errors
        data_access_layer.db_session.execute = AsyncMock(
            side_effect=OperationalError("connection failed", None, None)
        )
        with pytest.raises(OperationalError):
            await data_access_layer.test_connection()

    @pytest.mark.asyncio
    async def test_get_current_time(self, data_access_layer):
        """Verify timestamp retrieval for status endpoint."""
        result = await data_access_layer.get_current_time()
        assert result is not None
        assert isinstance(result, datetime)


# =============================================================================
# Metadata CRUD Tests
# =============================================================================


class TestMetadataCRUD:
    """Tests for Metadata CRUD operations."""

    @pytest.mark.asyncio
    async def test_get_metadata_exists(self, data_access_layer):
        """Retrieve existing metadata by GUID."""
        await data_access_layer.create_metadata(
            "test-get-guid", {"test": "data"}, {"authz": "/test"}
        )
        result = await data_access_layer.get_metadata("test-get-guid")
        assert result is not None
        assert result["guid"] == "test-get-guid"
        assert result["data"] == {"test": "data"}
        assert result["authz"] == {"authz": "/test"}

    @pytest.mark.asyncio
    async def test_get_metadata_not_found(self, data_access_layer):
        """Return None for non-existent GUID."""
        result = await data_access_layer.get_metadata("nonexistent-guid")
        assert result is None

    @pytest.mark.asyncio
    async def test_create_metadata_new(self, data_access_layer):
        """Insert new metadata record."""
        created, was_created = await data_access_layer.create_metadata(
            guid="new-guid", data={"new": "data"}, authz={"resource": "/new"}
        )
        assert was_created is True
        assert created["guid"] == "new-guid"
        assert created["data"] == {"new": "data"}
        assert created["authz"] == {"resource": "/new"}
        retrieved = await data_access_layer.get_metadata("new-guid")
        assert retrieved is not None
        assert retrieved["data"] == {"new": "data"}

    @pytest.mark.asyncio
    async def test_create_metadata_with_none_data(self, data_access_layer):
        """Insert metadata with None data."""
        created, was_created = await data_access_layer.create_metadata(
            guid="none-data-guid", data=None, authz={"resource": "/test"}
        )
        assert was_created is True
        assert created["guid"] == "none-data-guid"
        assert created["data"] is None

    @pytest.mark.asyncio
    async def test_create_metadata_duplicate_no_overwrite(self, data_access_layer):
        """Raise IntegrityError on duplicate without overwrite."""
        await data_access_layer.create_metadata("dup-guid", {"a": 1}, {"authz": "test"})
        with pytest.raises(IntegrityError):
            await data_access_layer.create_metadata(
                "dup-guid", {"b": 2}, {"authz": "test"}
            )

    @pytest.mark.asyncio
    async def test_create_metadata_duplicate_with_overwrite(self, data_access_layer):
        """Update existing on conflict with overwrite=True."""
        created1, was_created1 = await data_access_layer.create_metadata(
            "overwrite-guid", {"original": "data"}, {"authz": "test"}
        )
        assert was_created1 is True
        assert created1["data"] == {"original": "data"}
        # Overwrite
        created2, was_created2 = await data_access_layer.create_metadata(
            "overwrite-guid", {"new": "data"}, {"authz": "test"}, overwrite=True
        )
        assert was_created2 is False  # Updated not created
        assert created2["data"] == {"new": "data"}

    @pytest.mark.asyncio
    async def test_update_metadata_exists(self, data_access_layer):
        """Update existing metadata."""
        await data_access_layer.create_metadata(
            "update-guid", {"old": "data"}, {"authz": "test"}
        )
        result = await data_access_layer.update_metadata(
            "update-guid", {"new": "data2"}
        )
        assert result is not None
        assert result["data"] == {"new": "data2"}

    @pytest.mark.asyncio
    async def test_update_metadata_not_found(self, data_access_layer):
        """Return None for non-existent GUID."""
        result = await data_access_layer.update_metadata(
            "nonexistent-guid", {"new": "data"}
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_update_metadata_merge_true(self, data_access_layer):
        """Shallow merge with existing data."""
        await data_access_layer.create_metadata(
            "merge-guid", {"a": 1, "b": 2, "c": 3}, {"authz": "test"}
        )

        result = await data_access_layer.update_metadata(
            "merge-guid", {"b": 20, "d": 4}, merge=True
        )
        assert result is not None
        # a and c should be kept, b should be updated, d should be added
        assert result["data"] == {"a": 1, "b": 20, "c": 3, "d": 4}

    @pytest.mark.asyncio
    async def test_update_metadata_merge_false(self, data_access_layer):
        """Replace data entirely when merge=False."""
        await data_access_layer.create_metadata(
            "replace-guid", {"a": 1, "b": 2, "c": 3}, {"authz": "test"}
        )

        result = await data_access_layer.update_metadata(
            "replace-guid", {"x": 100}, merge=False
        )
        assert result is not None
        # All original data should be replaced
        assert result["data"] == {"x": 100}

    @pytest.mark.asyncio
    async def test_delete_metadata_exists(self, data_access_layer):
        """Delete existing metadata."""
        await data_access_layer.create_metadata(
            "del-guid", {"x": "y"}, {"authz": "test"}
        )

        deleted = await data_access_layer.delete_metadata("del-guid")
        assert deleted is not None
        assert deleted["guid"] == "del-guid"
        assert deleted["data"] == {"x": "y"}

        result = await data_access_layer.get_metadata("del-guid")
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_metadata_not_found(self, data_access_layer):
        """Return None for non-existent GUID."""
        result = await data_access_layer.delete_metadata("nonexistent-guid")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_metadata_by_alias_exists(self, data_access_layer):
        """Resolve alias to metadata."""
        await create_sample_data(data_access_layer)
        result = await data_access_layer.get_metadata_by_alias("sample_alias1")
        assert result is not None
        assert result["guid"] == "sample_guid1"
        assert result["data"] == {"key1": "value1", "nested": {"a": "b"}}

    @pytest.mark.asyncio
    async def test_get_metadata_by_alias_not_found(self, data_access_layer):
        """Return None for non-existent alias."""
        result = await data_access_layer.get_metadata_by_alias("nonexistent-alias")
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_metadata_cascades_aliases(self, data_access_layer):
        """
        Verify aliases are deleted with metadata.
        When parent row metadata is deleted, automatically delete all aliases child rows that reference it
        """
        await data_access_layer.create_metadata(
            "cascade-guid", {"test": "data"}, {"authz": "test"}
        )
        await data_access_layer.create_aliases(
            "cascade-guid", ["cascade-alias1", "cascade-alias2"]
        )

        aliases = await data_access_layer.get_aliases_for_guid("cascade-guid")
        assert len(aliases) == 2

        await data_access_layer.delete_metadata("cascade-guid")

        aliases = await data_access_layer.get_aliases_for_guid("cascade-guid")
        assert len(aliases) == 0


# =============================================================================
# Search Tests
# =============================================================================


class TestSearchMetadata:
    """Tests for metadata search functionality."""

    @pytest.mark.asyncio
    async def test_search_metadata_no_filters(self, data_access_layer):
        """Return all records with pagination."""
        await create_sample_data(data_access_layer)
        result = await data_access_layer.search_metadata(
            filters={}, limit=10, offset=0, return_data=False
        )
        assert isinstance(result, list)
        assert len(result) == 3
        assert "sample_guid1" in result
        assert "sample_guid2" in result
        assert "sample_guid3" in result

    @pytest.mark.asyncio
    async def test_search_metadata_simple_filter(self, data_access_layer):
        """Filter by single path."""
        await create_sample_data(data_access_layer)
        result = await data_access_layer.search_metadata(
            filters={"key1": ["value1"]}, limit=10, offset=0, return_data=False
        )
        assert isinstance(result, list)
        assert "sample_guid1" in result
        assert "sample_guid3" in result
        assert "sample_guid2" not in result

    @pytest.mark.asyncio
    async def test_search_metadata_nested_filter(self, data_access_layer):
        """Filter by nested path (dot notation)."""
        await create_sample_data(data_access_layer)
        result = await data_access_layer.search_metadata(
            filters={"nested.a": ["b"]}, limit=10, offset=0, return_data=False
        )
        assert isinstance(result, list)
        assert "sample_guid1" in result
        assert "sample_guid2" not in result
        assert "sample_guid3" not in result

    @pytest.mark.asyncio
    async def test_search_metadata_wildcard_filter(self, data_access_layer):
        """Filter by key existence (*)."""
        await create_sample_data(data_access_layer)
        result = await data_access_layer.search_metadata(
            filters={"key2": ["*"]}, limit=10, offset=0, return_data=False
        )
        assert isinstance(result, list)
        assert "sample_guid2" in result
        assert "sample_guid1" not in result
        assert "sample_guid3" not in result

    @pytest.mark.asyncio
    async def test_search_metadata_escaped_asterisk(self, data_access_layer):
        """Match literal asterisk (\\*)."""
        await data_access_layer.create_metadata(
            "asterisk-guid", {"star": "*"}, {"authz": "test"}
        )

        result = await data_access_layer.search_metadata(
            filters={"star": ["\\*"]}, limit=10, offset=0, return_data=False
        )
        assert isinstance(result, list)
        assert "asterisk-guid" in result

    @pytest.mark.asyncio
    async def test_search_metadata_multiple_values(self, data_access_layer):
        """OR logic for multiple values."""
        await create_sample_data(data_access_layer)
        result = await data_access_layer.search_metadata(
            filters={"key1": ["value1", "nonexistent"]},
            limit=10,
            offset=0,
            return_data=False,
        )
        assert isinstance(result, list)
        assert "sample_guid1" in result
        assert "sample_guid3" in result

    @pytest.mark.asyncio
    async def test_search_metadata_return_data_true(self, data_access_layer):
        """Return dict[guid, data] when return_data=True."""
        await create_sample_data(data_access_layer)
        result = await data_access_layer.search_metadata(
            filters={"key1": ["value1"]}, limit=10, offset=0, return_data=True
        )
        assert isinstance(result, dict)
        assert "sample_guid1" in result
        assert result["sample_guid1"] == {"key1": "value1", "nested": {"a": "b"}}

    @pytest.mark.asyncio
    async def test_search_metadata_return_data_false(self, data_access_layer):
        """Return list[guid] when return_data=False."""
        await create_sample_data(data_access_layer)
        result = await data_access_layer.search_metadata(
            filters={"key1": ["value1"]}, limit=10, offset=0, return_data=False
        )
        assert isinstance(result, list)
        assert "sample_guid1" in result

    @pytest.mark.asyncio
    async def test_search_metadata_pagination(self, data_access_layer):
        """
        Verify limit and offset work correctly.

        TODO/FIXME: there are known limitations in search_metadata(),
        due to no no updated date on the records
        """
        for i in range(5):
            await data_access_layer.create_metadata(
                f"page-guid-{i:02d}", {"index": i}, {"authz": "test"}
            )

        # Get first page
        page1 = await data_access_layer.search_metadata(
            filters={"index": ["*"]}, limit=2, offset=0, return_data=False
        )
        assert len(page1) == 2

        # Get second page
        page2 = await data_access_layer.search_metadata(
            filters={"index": ["*"]}, limit=2, offset=2, return_data=False
        )
        assert len(page2) == 2

        # Pages should not overlap
        assert set(page1).isdisjoint(set(page2))


# =============================================================================
# Alias Operations Tests
# =============================================================================


class TestAliasOperations:
    """Tests for alias operations."""

    @pytest.mark.asyncio
    async def test_get_aliases_for_guid_exists(self, data_access_layer):
        """Return list of aliases."""
        await create_sample_data(data_access_layer)
        aliases = await data_access_layer.get_aliases_for_guid("sample_guid1")
        assert len(aliases) == 2
        assert "sample_alias1" in aliases
        assert "sample_alias1a" in aliases

    @pytest.mark.asyncio
    async def test_get_aliases_for_guid_none(self, data_access_layer):
        """Return empty list if no aliases."""
        await create_sample_data(data_access_layer)
        aliases = await data_access_layer.get_aliases_for_guid("sample_guid3")
        assert aliases == []

    @pytest.mark.asyncio
    async def test_create_aliases_success(self, data_access_layer):
        """Create new aliases."""
        await data_access_layer.create_metadata(
            "alias-test-guid", {"test": "data"}, {"authz": "test"}
        )
        aliases = await data_access_layer.create_aliases(
            "alias-test-guid", ["new-alias-1", "new-alias-2"]
        )

        assert len(aliases) == 2
        assert "new-alias-1" in aliases
        assert "new-alias-2" in aliases

        retrieved = await data_access_layer.get_aliases_for_guid("alias-test-guid")
        assert len(retrieved) == 2

    @pytest.mark.asyncio
    async def test_create_aliases_duplicate_in_list(self, data_access_layer):
        """Handle duplicates in input list."""
        await data_access_layer.create_metadata(
            "dup-alias-guid", {"test": "data"}, {"authz": "test"}
        )
        aliases = await data_access_layer.create_aliases(
            "dup-alias-guid", ["same-alias", "same-alias", "same-alias"]
        )

        assert len(aliases) == 1
        assert "same-alias" in aliases

    @pytest.mark.asyncio
    async def test_create_aliases_guid_not_found(self, data_access_layer):
        """Raise IntegrityError for non-existent GUID."""
        with pytest.raises(IntegrityError):
            await data_access_layer.create_aliases("nonexistent-guid", ["some-alias"])

    @pytest.mark.asyncio
    async def test_create_aliases_already_exists(self, data_access_layer):
        """Raise IntegrityError for existing alias."""
        await create_sample_data(data_access_layer)
        # sample_alias1 already exists for sample_guid1
        with pytest.raises(IntegrityError):
            await data_access_layer.create_aliases("sample_guid2", ["sample_alias1"])

    @pytest.mark.asyncio
    async def test_update_aliases_replace(self, data_access_layer):
        """Replace all aliases (merge=False)."""
        await data_access_layer.create_metadata(
            "update-alias-guid", {"test": "data"}, {"authz": "test"}
        )
        await data_access_layer.create_aliases(
            "update-alias-guid", ["old-alias-1", "old-alias-2"]
        )

        # Replace with new aliases
        result = await data_access_layer.update_aliases(
            "update-alias-guid", ["new-alias-1", "new-alias-2"], merge=False
        )

        assert "new-alias-1" in result
        assert "new-alias-2" in result
        assert "old-alias-1" not in result
        assert "old-alias-2" not in result

    @pytest.mark.asyncio
    async def test_update_aliases_merge(self, data_access_layer):
        """Add new aliases, keep existing (merge=True)."""
        await data_access_layer.create_metadata(
            "merge-alias-guid", {"test": "data"}, {"authz": "test"}
        )
        await data_access_layer.create_aliases("merge-alias-guid", ["existing-alias"])

        # Merge with new aliases
        result = await data_access_layer.update_aliases(
            "merge-alias-guid", ["new-alias"], merge=True
        )

        assert "existing-alias" in result
        assert "new-alias" in result

    @pytest.mark.asyncio
    async def test_delete_alias_exists(self, data_access_layer):
        """Delete specific alias."""
        await data_access_layer.create_metadata(
            "del-alias-guid", {"test": "data"}, {"authz": "test"}
        )
        await data_access_layer.create_aliases(
            "del-alias-guid", ["to-delete", "to-keep"]
        )

        result = await data_access_layer.delete_alias("del-alias-guid", "to-delete")
        assert result is not None
        assert result["alias"] == "to-delete"

        # Verify only to-keep remains
        remaining = await data_access_layer.get_aliases_for_guid("del-alias-guid")
        assert remaining == ["to-keep"]

    @pytest.mark.asyncio
    async def test_delete_alias_not_found(self, data_access_layer):
        """Return None for non-existent alias"""
        await data_access_layer.create_metadata(
            "del-alias-guid2", {"test": "data"}, {"authz": "test"}
        )
        result = await data_access_layer.delete_alias(
            "del-alias-guid2", "nonexistent-alias"
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_all_aliases(self, data_access_layer):
        """Delete all aliases for GUID."""
        await data_access_layer.create_metadata(
            "del-all-guid", {"test": "data"}, {"authz": "test"}
        )
        await data_access_layer.create_aliases(
            "del-all-guid", ["alias-a", "alias-b", "alias-c"]
        )

        number_deleted = await data_access_layer.delete_all_aliases("del-all-guid")
        assert number_deleted == 3

        # Verify all are gone
        remaining = await data_access_layer.get_aliases_for_guid("del-all-guid")
        assert remaining == []


# =============================================================================
# Batch Operations Tests
# =============================================================================


class TestBatchOperations:
    """Tests for batch operations."""

    @pytest.mark.asyncio
    async def test_batch_create_metadata_all_new(self, data_access_layer):
        """Create multiple new records."""
        data_list = [
            {"guid": "batch-1", "data": {"a": 1}},
            {"guid": "batch-2", "data": {"b": 2}},
            {"guid": "batch-3", "data": {"c": 3}},
        ]

        result = await data_access_layer.batch_create_metadata(
            data_list, overwrite=False, default_authz={"authz": "test"}
        )

        assert len(result["created"]) == 3
        assert "batch-1" in result["created"]
        assert "batch-2" in result["created"]
        assert "batch-3" in result["created"]
        assert len(result["updated"]) == 0
        assert len(result["conflict"]) == 0
        assert len(result["bad_input"]) == 0

    @pytest.mark.asyncio
    async def test_batch_create_metadata_with_overwrite(self, data_access_layer):
        """Update existing records with overwrite=True."""
        # Create initial record
        await data_access_layer.create_metadata(
            "batch-existing", {"old": "data"}, {"authz": "test"}
        )

        data_list = [
            {"guid": "batch-existing", "data": {"new": "data"}},
            {"guid": "batch-new", "data": {"fresh": "data"}},
        ]

        result = await data_access_layer.batch_create_metadata(
            data_list, overwrite=True, default_authz={"authz": "test"}
        )

        assert "batch-new" in result["created"]
        assert "batch-existing" in result["updated"]

    @pytest.mark.asyncio
    async def test_batch_create_metadata_forbidden_ids(self, data_access_layer):
        """Filter out forbidden GUIDs."""
        data_list = [
            {"guid": "allowed-guid", "data": {"ok": True}},
            {"guid": "forbidden-guid", "data": {"not": "allowed"}},
        ]

        result = await data_access_layer.batch_create_metadata(
            data_list,
            overwrite=False,
            default_authz={"authz": "test"},
            forbidden_ids={"forbidden-guid"},
        )

        assert "allowed-guid" in result["created"]
        assert "forbidden-guid" in result["bad_input"]
        assert "forbidden-guid" not in result["created"]

    @pytest.mark.asyncio
    async def test_batch_create_metadata_mixed_results(self, data_access_layer):
        """Handle mix of created/updated/conflict."""
        # Create existing record
        await data_access_layer.create_metadata(
            "batch-mix-existing", {"old": "data"}, {"authz": "test"}
        )

        data_list = [
            {"guid": "batch-mix-new", "data": {"new": "data"}},
            {"guid": "batch-mix-existing", "data": {"updated": "data"}},
        ]

        result = await data_access_layer.batch_create_metadata(
            data_list, overwrite=True, default_authz={"authz": "test"}
        )

        assert "batch-mix-new" in result["created"]
        assert "batch-mix-existing" in result["updated"]

    @pytest.mark.asyncio
    async def test_batch_create_metadata_conflict_no_overwrite(self, data_access_layer):
        """
        Test that duplicate GUIDs are reported as conflicts when overwrite=False.
        """
        # Create existing records
        await data_access_layer.create_metadata(
            "existing-1", {"old": "data1"}, {"authz": "test"}
        )
        await data_access_layer.create_metadata(
            "existing-2", {"old": "data2"}, {"authz": "test"}
        )

        data_list = [
            {"guid": "new-guid", "data": {"new": "data"}},
            {"guid": "existing-1", "data": {"try": "overwrite"}},
            {"guid": "another-new", "data": {"more": "data"}},
            {"guid": "existing-2", "data": {"also": "conflict"}},
        ]

        result = await data_access_layer.batch_create_metadata(
            data_list, overwrite=False, default_authz={"authz": "test"}
        )

        # New GUIDs should be created
        assert "new-guid" in result["created"]
        assert "another-new" in result["created"]
        assert len(result["created"]) == 2

        # Existing GUIDs should be reported as conflicts
        assert "existing-1" in result["conflict"]
        assert "existing-2" in result["conflict"]
        assert len(result["conflict"]) == 2

        # Nothing should be updated (overwrite=False)
        assert len(result["updated"]) == 0

        # Verify original data was not modified
        existing1 = await data_access_layer.get_metadata("existing-1")
        assert existing1["data"] == {"old": "data1"}

        existing2 = await data_access_layer.get_metadata("existing-2")
        assert existing2["data"] == {"old": "data2"}


# =============================================================================
# Index Operations Tests
# =============================================================================


class TestIndexOperations:
    """Tests for index operations."""

    @pytest.mark.asyncio
    async def test_list_metadata_indexes_empty(self, data_access_layer):
        """Return empty list when no custom indexes exist."""
        # Fixture clears all custom indexes, so list should be empty
        result = await data_access_layer.list_metadata_indexes()
        assert result == []

    @pytest.mark.asyncio
    async def test_create_metadata_index_success(self, data_access_layer):
        """Create new index."""
        path = "test_index_field"
        result = await data_access_layer.create_metadata_index(path)
        assert result == path

        indexes = await data_access_layer.list_metadata_indexes()
        assert path in indexes

        # Clean up
        await data_access_layer.drop_metadata_index(path)

    @pytest.mark.asyncio
    async def test_create_metadata_index_nested_path(self, data_access_layer):
        """Create index on nested JSON path."""
        path = "nested.deep.field"
        result = await data_access_layer.create_metadata_index(path)
        assert result == path

        indexes = await data_access_layer.list_metadata_indexes()
        assert path in indexes

        # Clean up
        await data_access_layer.drop_metadata_index(path)

    @pytest.mark.asyncio
    async def test_create_metadata_index_duplicate(self, data_access_layer):
        """Raise ProgrammingError when index already exists."""
        path = "duplicate_index_field"

        await data_access_layer.create_metadata_index(path)

        with pytest.raises(ProgrammingError) as exc_info:
            await data_access_layer.create_metadata_index(path)

        assert "already exists" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_drop_metadata_index_success(self, data_access_layer):
        """Drop existing index (simple and nested paths)."""
        # Test simple path
        simple_path = "to_drop_field"
        await data_access_layer.create_metadata_index(simple_path)

        indexes = await data_access_layer.list_metadata_indexes()
        assert simple_path in indexes

        await data_access_layer.drop_metadata_index(simple_path)

        indexes = await data_access_layer.list_metadata_indexes()
        assert simple_path not in indexes

        # Test nested path
        nested_path = "nested.deep.to_drop"
        await data_access_layer.create_metadata_index(nested_path)

        indexes = await data_access_layer.list_metadata_indexes()
        assert nested_path in indexes

        await data_access_layer.drop_metadata_index(nested_path)

        indexes = await data_access_layer.list_metadata_indexes()
        assert nested_path not in indexes

    @pytest.mark.asyncio
    async def test_drop_metadata_index_not_found(self, data_access_layer):
        """Raise sqlalchemy ProgrammingError for non-existent index."""
        with pytest.raises(ProgrammingError):
            await data_access_layer.drop_metadata_index("nonexistent_index_path")

    @pytest.mark.asyncio
    async def test_list_metadata_indexes_with_indexes(self, data_access_layer):
        """Return paths of existing indexes."""
        path1 = "list_test_field_1"
        path2 = "list_test_field_2"

        await data_access_layer.create_metadata_index(path1)
        await data_access_layer.create_metadata_index(path2)

        indexes = await data_access_layer.list_metadata_indexes()
        assert path1 in indexes
        assert path2 in indexes

        await data_access_layer.drop_metadata_index(path1)
        await data_access_layer.drop_metadata_index(path2)
