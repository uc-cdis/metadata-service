from typing import Optional

from pydantic import BaseModel, ConfigDict
from sqlalchemy import Column, ForeignKey, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


# =============================================================================
# SQLAlchemy ORM Models
# =============================================================================


class Metadata(Base):
    """
    Core metadata table storing GUID-indexed JSON metadata with authorization info.

    Attributes:
        guid (str): Primary key, unique identifier for the metadata record
        data: JSONB column containing the actual metadata
        authz: JSONB column containing authorization information (required)
    """

    __tablename__ = "metadata"

    guid = Column(String, primary_key=True)
    data = Column(JSONB(), nullable=True)
    authz = Column(JSONB(), nullable=False)

    # Relationship to aliases (one-to-many)
    aliases = relationship(
        "MetadataAlias",
        back_populates="metadata_record",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    def to_dict(self) -> dict:
        """Convert model instance to dictionary."""
        return {
            "guid": self.guid,
            "data": self.data,
            "authz": self.authz,
        }

    def __repr__(self) -> str:
        return f"<Metadata(guid={self.guid!r})>"


class MetadataAlias(Base):
    """
    Alias table for mapping alternative identifiers to metadata GUIDs.

    Attributes:
        alias (str): Primary key, the alternative identifier
        guid (str): Foreign key referencing metadata.guid (cascades on delete)
    """

    __tablename__ = "metadata_alias"

    alias = Column(String, primary_key=True)
    guid = Column(
        String,
        ForeignKey("metadata.guid", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Relationship back to metadata (many-to-one)
    # NOTE: 'metadata' is a reserved attribute, use metadata_record
    metadata_record = relationship("Metadata", back_populates="aliases")

    def to_dict(self) -> dict:
        """Convert model instance to dictionary."""
        return {
            "alias": self.alias,
            "guid": self.guid,
        }

    def __repr__(self) -> str:
        return f"<MetadataAlias(alias={self.alias!r}, guid={self.guid!r})>"


# =============================================================================
# Pydantic Input Models for API Validation
# =============================================================================


class MetadataInput(BaseModel):
    """
    Pydantic model for metadata creation/update input validation.
    """

    model_config = ConfigDict(extra="forbid")

    data: Optional[dict] = None
    authz: Optional[dict] = None


class MetadataCreateInput(BaseModel):
    """
    Pydantic model for metadata creation with required authz.
    """

    model_config = ConfigDict(extra="forbid")

    data: Optional[dict] = None
    # Required for creation
    authz: dict


class MetadataBatchInput(BaseModel):
    """
    Pydantic model for batch metadata operations.
    Each key in the dict is a GUID, and the value is the metadata.
    """

    model_config = ConfigDict(extra="forbid")

    # The actual batch data is passed as the request body directly,
    # so this model is a placeholder for potential future extensions


class AliasInput(BaseModel):
    """
    Pydantic model for alias creation/update input validation.
    """

    model_config = ConfigDict(extra="forbid")

    aliases: list[str]


class AliasCreateInput(BaseModel):
    """
    Pydantic model for creating a single alias.
    """

    model_config = ConfigDict(extra="forbid")

    alias: str


# =============================================================================
# Response Models (for API documentation)
# =============================================================================


class MetadataResponse(BaseModel):
    """
    Pydantic model for metadata response.
    """

    model_config = ConfigDict(from_attributes=True)

    guid: str
    data: Optional[dict] = None
    authz: dict


class AliasResponse(BaseModel):
    """
    Pydantic model for alias response.
    """

    model_config = ConfigDict(from_attributes=True)

    alias: str
    guid: str


class AliasListResponse(BaseModel):
    """
    Pydantic model for listing aliases.
    """

    aliases: list[str]
