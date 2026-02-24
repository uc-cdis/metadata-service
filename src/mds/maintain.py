import json

from fastapi import HTTPException, APIRouter, Depends
from sqlalchemy.exc import IntegrityError
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.status import (
    HTTP_201_CREATED,
    HTTP_400_BAD_REQUEST,
    HTTP_404_NOT_FOUND,
    HTTP_409_CONFLICT,
)

from .admin_login import admin_required
from . import config
from .objects import FORBIDDEN_IDS
from .db import get_data_access_layer, DataAccessLayer

mod = APIRouter()


@mod.post("/metadata")
async def batch_create_metadata(
    request: Request,
    data_list: list[dict],
    overwrite: bool = True,
    data_access_layer: DataAccessLayer = Depends(get_data_access_layer),
):
    """Create metadata in batch."""
    request.scope.get("add_close_watcher", lambda: None)()
    authz = json.loads(config.DEFAULT_AUTHZ_STR)

    result = await data_access_layer.batch_create_metadata(
        data_list=data_list,
        overwrite=overwrite,
        default_authz=authz,
        forbidden_ids=FORBIDDEN_IDS,
    )

    return JSONResponse(content=result, status_code=HTTP_201_CREATED)


@mod.post("/metadata/{guid:path}")
async def create_metadata(
    guid,
    data: dict,
    overwrite: bool = False,
    data_access_layer: DataAccessLayer = Depends(get_data_access_layer),
):
    """Create metadata for the GUID."""
    authz = json.loads(config.DEFAULT_AUTHZ_STR)

    # GUID should not be in the FORBIDDEN_ID list (eg, 'upload').
    # This will help avoid conflicts between
    # POST /api/v1/objects/{GUID or ALIAS} and POST /api/v1/objects/upload endpoints
    if guid in FORBIDDEN_IDS:
        raise HTTPException(
            HTTP_400_BAD_REQUEST, "GUID cannot have value: {FORBIDDEN_IDS}"
        )

    try:
        record, created = await data_access_layer.create_metadata(
            guid=guid, data=data, authz=authz, overwrite=overwrite
        )
    except Exception as e:
        # Check for duplicate GUID error
        if isinstance(e, IntegrityError):
            raise HTTPException(HTTP_409_CONFLICT, f"Conflict: {guid}")
        raise

    if created:
        return JSONResponse(content=record["data"], status_code=HTTP_201_CREATED)
    else:
        return record["data"]


@mod.put("/metadata/{guid:path}")
async def update_metadata(
    guid,
    data: dict,
    merge: bool = False,
    data_access_layer: DataAccessLayer = Depends(get_data_access_layer),
):
    """Update the metadata of the GUID.

    If `merge` is True, then any top-level keys that are not in the new data will be
    kept, and those that also exist in the new data will be replaced completely. This
    is also known as the shallow merge. The metadata service currently doesn't support
    deep merge.
    """
    # TODO PUT should create if it doesn't exist...
    updated_record = await data_access_layer.update_metadata(
        guid=guid, data=data, merge=merge
    )
    if updated_record:
        return updated_record["data"]
    else:
        raise HTTPException(HTTP_404_NOT_FOUND, f"Not found: {guid}")


@mod.delete("/metadata/{guid:path}")
async def delete_metadata(
    guid,
    data_access_layer: DataAccessLayer = Depends(get_data_access_layer),
):
    """Delete the metadata of the GUID."""
    deleted_record = await data_access_layer.delete_metadata(guid=guid)
    if deleted_record:
        return deleted_record["data"]
    else:
        raise HTTPException(HTTP_404_NOT_FOUND, f"Not found: {guid}")


def init_app(app):
    app.include_router(mod, tags=["Maintain"], dependencies=[Depends(admin_required)])
