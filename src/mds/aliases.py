"""
Support for aliases (alternative, unique names) for Metadata blobs
that already have a Globally Unique IDentifier (GUID).

It is always more efficient to use GUIDs as primary method
for naming blobs. However, in cases where you want multiple identifiers
to point to the same blob, aliases allow that without duplicating the
actual blob.
"""
from typing import Union

from asyncpg import UniqueViolationError, ForeignKeyViolationError
from fastapi import HTTPException, APIRouter, Depends
from pydantic import BaseModel
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.status import (
    HTTP_201_CREATED,
    HTTP_204_NO_CONTENT,
    HTTP_404_NOT_FOUND,
    HTTP_409_CONFLICT,
)

from .admin_login import admin_required
from .models import MetadataAlias
from . import logger

mod = APIRouter()


class AliasObjInput(BaseModel):
    """
    Alias object

    aliases (list, optional): unique names to allow using in place of whatever GUID
        specified
    """

    aliases: list[str] = None


@mod.post("/metadata/{guid:path}/aliases")
async def create_metadata_aliases(
    guid: str,
    body: AliasObjInput,
    request: Request,
) -> JSONResponse:
    """
    Create metadata aliases for the GUID.

    Args:
        guid (str): Metadata GUID
        body (AliasObjInput): JSON body with list of aliases
        request (Request): incoming request
    """
    input_body_aliases = body.aliases or []
    aliases = list(set(input_body_aliases))

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
    except UniqueViolationError as exc:
        logger.debug(exc)
        raise HTTPException(
            HTTP_409_CONFLICT,
            f"Alias: '{alias}' is already in use by a different GUID.",
        )

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
        merge (bool, optional): If `merge` is True, then any aliases that are not
            in the new data will be kept.
    """
    input_body_aliases = body.aliases or []
    requested_aliases = set(input_body_aliases)
    logger.debug(f"requested_aliases: {requested_aliases}")

    existing_metadata_aliases = await MetadataAlias.query.where(
        MetadataAlias.guid == guid
    ).gino.all()

    existing_aliases = set([item.alias for item in existing_metadata_aliases])
    aliases_to_add = requested_aliases - existing_aliases
    final_aliases = requested_aliases | existing_aliases

    if not merge:
        # remove old aliases if they don't exist in new ones
        for alias_metadata in existing_metadata_aliases:
            if alias_metadata.alias not in requested_aliases:
                logger.debug(
                    f"deleting MetadataAlias(alias={alias_metadata.alias}, guid={guid})"
                )
                await alias_metadata.delete()
        final_aliases = requested_aliases

    for alias in aliases_to_add:
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
        except UniqueViolationError as exc:
            logger.debug(exc)
            raise HTTPException(
                HTTP_409_CONFLICT,
                f"Alias: '{alias}' is already in use by a different GUID.",
            )

    return JSONResponse(
        {"guid": guid, "aliases": sorted(list(final_aliases))}, HTTP_201_CREATED
    )


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
    logger.debug(f"deleting: {metadata_alias}")
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
