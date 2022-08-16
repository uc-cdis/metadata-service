"""
Support for aliases (alternative, unique names) for Metadata blobs
that already have a Globally Unique IDentifier (GUID).

It is always more efficient to use GUIDs as primary method
for naming blobs. However in cases where you want multiple identifiers
to point to the same blob, aliases allow that without duplicating the
actual blob.
"""
import json
import re

from asyncpg import UniqueViolationError, ForeignKeyViolationError
from fastapi import HTTPException, APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import bindparam
from sqlalchemy.dialects.postgresql import insert
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.status import (
    HTTP_200_OK,
    HTTP_201_CREATED,
    HTTP_204_NO_CONTENT,
    HTTP_400_BAD_REQUEST,
    HTTP_404_NOT_FOUND,
    HTTP_409_CONFLICT,
)
import urllib.parse

from .admin_login import admin_required
from .models import db, MetadataAlias, Metadata
from . import config, logger
from .objects import FORBIDDEN_IDS

mod = APIRouter()


class AliasObjInput(BaseModel):
    """
    Alias object

    aliases (list, optional): unique names to allow using in place of whatever GUID
        specified
    """

    aliases: list = None


@mod.post("/metadata/{guid:path}/aliases")
async def create_metadata_aliases(
    guid: str,
    body: AliasObjInput,
    request: Request,
) -> JSONResponse:
    """Create metadata aliases for the GUID.

    Args:
        guid (str): Metadata GUID
        body (AliasObjInput): JSON body with list of aliases
        request (Request): incoming request
    """
    aliases = list(set(body.aliases)) or []

    metadata_aliases = await MetadataAlias.query.where(
        MetadataAlias.guid == guid
    ).gino.all()

    if metadata_aliases:
        raise HTTPException(
            HTTP_409_CONFLICT,
            f"Aliases already exist for {guid}. " "Use PUT to overwrite.",
        )

    try:
        for alias in aliases:
            logger.debug(f"inserting MetadataAlias(alias={alias}, guid={guid})")
            metadata_alias = (
                await MetadataAlias.insert()
                .values(alias=alias, guid=guid)
                .returning(*MetadataAlias)
                .gino.first()
            )
    except ForeignKeyViolationError as exc:
        logger.debug(exc)
        raise HTTPException(HTTP_404_NOT_FOUND, f"GUID: '{guid}' does not exist.")

    return JSONResponse({"guid": guid, "aliases": sorted(aliases)}, HTTP_201_CREATED)


@mod.put("/metadata/{guid:path}/aliases")
async def update_metadata_alias(
    guid: str,
    body: AliasObjInput,
    request: Request,
    merge: bool = False,
) -> JSONResponse:
    """
    Update the metadata aliases of the GUID.

    If `merge` is True, then any aliases that are not in the new data will be
    kept.

    Args:
        guid (str): Metadata GUID
        body (AliasObjInput): JSON body with list of aliases
        request (Request): incoming request
        merge (bool, optional): Description
    """
    # TODO PUT should create if it doesn't exist...
    existing_metadata_aliases = await MetadataAlias.query.where(
        MetadataAlias.guid == guid
    ).gino.all()
    existing_aliases = [item.alias for item in existing_metadata_aliases]
    new_aliases = list(set(body.aliases)) or []

    if merge:
        aliases = list(set(existing_aliases) | set(new_aliases))
    else:
        # remove old aliases if they don't exist in new ones
        for alias_metadata in existing_metadata_aliases:
            if alias_metadata.alias not in new_aliases:
                logger.debug(
                    f"deleting MetadataAlias(alias={alias_metadata.alias}, guid={guid})"
                )
                await alias_metadata.delete()

        aliases = new_aliases

    for alias in aliases:
        # don't create a new entry if one already exists
        if alias in existing_aliases:
            logger.debug(f"already exists MetadataAlias(alias={alias}, guid={guid})")
            continue

        try:
            logger.debug(f"inserting MetadataAlias(alias={alias}, guid={guid})")
            metadata_alias = (
                await MetadataAlias.insert()
                .values(alias=alias, guid=guid)
                .returning(*MetadataAlias)
                .gino.first()
            )
        except ForeignKeyViolationError as exc:
            logger.debug(exc)
            raise HTTPException(HTTP_404_NOT_FOUND, f"GUID: '{guid}' does not exist.")

    return JSONResponse({"guid": guid, "aliases": sorted(aliases)}, HTTP_201_CREATED)


@mod.delete("/metadata/{guid:path}/aliases/{alias:path}")
async def delete_metadata_alias(
    guid: str,
    alias: str,
    request: Request,
) -> JSONResponse:
    """
    Delete the specified metadata_alias of the GUID.

    Args:
        guid (str): Metadata GUID
        alias (str): the metadata alias to delete
        request (Request): incoming request
    """
    metadata_alias = (
        await MetadataAlias.delete.where(MetadataAlias.guid == guid)
        .where(MetadataAlias.alias == alias)
        .returning(*MetadataAlias)
        .gino.first()
    )
    logger.info(metadata_alias)
    if metadata_alias:
        return JSONResponse({}, HTTP_204_NO_CONTENT)
    else:
        raise HTTPException(
            HTTP_404_NOT_FOUND, f"Alias: '{alias}' for GUID: '{guid}' not found."
        )


@mod.delete("/metadata/{guid:path}/aliases")
async def delete_all_metadata_aliases(
    guid: str,
    request: Request,
) -> JSONResponse:
    """
    Delete all metadata_aliases of the GUID.

    Args:
        guid (str): Metadata GUID
        request (Request): incoming request
    """
    metadata_alias = (
        await MetadataAlias.delete.where(MetadataAlias.guid == guid)
        .returning(*MetadataAlias)
        .gino.first()
    )
    if metadata_alias:
        return JSONResponse({}, HTTP_204_NO_CONTENT)
    else:
        raise HTTPException(HTTP_404_NOT_FOUND, f"No aliases found for: {guid}")


def init_app(app):
    app.include_router(mod, tags=["Aliases"], dependencies=[Depends(admin_required)])
