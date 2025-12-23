"""
Support for aliases (alternative, unique names) for Metadata blobs
that already have a Globally Unique IDentifier (GUID).

It is always more efficient to use GUIDs as primary method
for naming blobs. However, in cases where you want multiple identifiers
to point to the same blob, aliases allow that without duplicating the
actual blob.
"""
from fastapi import HTTPException, APIRouter, Depends
from sqlalchemy.exc import IntegrityError
from pydantic import BaseModel
from starlette.responses import JSONResponse
from starlette.status import (
    HTTP_201_CREATED,
    HTTP_204_NO_CONTENT,
    HTTP_404_NOT_FOUND,
    HTTP_409_CONFLICT,
)

from .admin_login import admin_required
from .db import get_data_access_layer, DataAccessLayer
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
    dal: DataAccessLayer = Depends(get_data_access_layer),
) -> JSONResponse:
    """
    Create metadata aliases for the GUID.

    Args:
        guid (str): Metadata GUID
        body (AliasObjInput): JSON body with list of aliases
        request (Request): incoming request
    """
    aliases = list(set(body.aliases or []))

    try:
        alias_list = await dal.get_aliases_for_guid(guid)
        if alias_list:
            raise HTTPException(
                HTTP_409_CONFLICT,
                f"Aliases already exist for {guid}. Use PUT to overwrite.",
            )
        await dal.create_aliases(guid, aliases)
    except IntegrityError as exc:
        # This specifically catches DB unique violation
        if "unique" in str(exc).lower() or "duplicate" in str(exc).lower():
            raise HTTPException(
                HTTP_409_CONFLICT,
                "Alias is already in use by a different GUID.",
            )
        raise

    return JSONResponse({"guid": guid, "aliases": sorted(aliases)}, HTTP_201_CREATED)


@mod.put("/metadata/{guid:path}/aliases")
async def update_metadata_alias(
    guid: str,
    body: AliasObjInput,
    merge: bool = False,
    dal: DataAccessLayer = Depends(get_data_access_layer),
) -> JSONResponse:
    """
    Update the metadata aliases of the GUID.

    If `merge` is True, then any aliases that are not in the new data will be
    kept.

    Args:
        guid (str): Metadata GUID
        body (AliasObjInput): JSON body with list of aliases
        merge (bool, optional): If `merge` is True, then any aliases that are not
            in the new data will be kept.
    """
    requested_aliases = set(body.aliases or [])
    final_aliases = await dal.update_aliases(guid, requested_aliases, merge=merge)

    return JSONResponse({"guid": guid, "aliases": final_aliases}, HTTP_201_CREATED)


@mod.delete("/metadata/{guid:path}/aliases/{alias:path}")
async def delete_metadata_alias(
    guid: str,
    alias: str,
    dal: DataAccessLayer = Depends(get_data_access_layer),
) -> JSONResponse:
    """
    Delete the specified metadata_alias of the GUID.

    Args:
        guid (str): Metadata GUID
        alias (str): the metadata alias to delete
    """
    deleted_alias = await dal.delete_alias(guid, alias)
    if deleted_alias:
        return JSONResponse({}, HTTP_204_NO_CONTENT)
    else:
        raise HTTPException(
            HTTP_404_NOT_FOUND, f"Alias: '{alias}' for GUID: '{guid}' not found."
        )


@mod.delete("/metadata/{guid:path}/aliases")
async def delete_all_metadata_aliases(
    guid: str,
    dal: DataAccessLayer = Depends(get_data_access_layer),
) -> JSONResponse:
    """
    Delete all metadata_aliases of the GUID.

    Args:
        guid (str): Metadata GUID
    """
    count = await dal.delete_all_aliases(guid)
    if count > 0:
        return JSONResponse({}, HTTP_204_NO_CONTENT)
    else:
        raise HTTPException(HTTP_404_NOT_FOUND, f"No aliases found for: {guid}")


def init_app(app):
    app.include_router(mod, tags=["Aliases"], dependencies=[Depends(admin_required)])
