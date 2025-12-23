import re

from sqlalchemy.exc import ProgrammingError
from fastapi import HTTPException, APIRouter, Depends
from starlette.status import (
    HTTP_201_CREATED,
    HTTP_204_NO_CONTENT,
    HTTP_404_NOT_FOUND,
    HTTP_409_CONFLICT,
)

from .admin_login import admin_required
from .db import get_data_access_layer, DataAccessLayer

INDEX_REGEXP = re.compile(r"data #>> '{(.+)}'::text")

mod = APIRouter()


@mod.get("/metadata_index")
async def list_metadata_indexes(dal: DataAccessLayer = Depends(get_data_access_layer)):
    """List all the metadata key paths indexed in the database."""
    rv = await dal.list_metadata_indexes()
    return rv


@mod.post("/metadata_index/{path}", status_code=HTTP_201_CREATED)
async def create_metadata_indexes(
    path,
    dal: DataAccessLayer = Depends(get_data_access_layer),
):
    """Create a database index on the given metadata key path."""
    try:
        rv = await dal.create_metadata_index(path)
    except ProgrammingError:
        raise HTTPException(HTTP_409_CONFLICT, f"Conflict: {path}")
    return rv


@mod.delete("/metadata_index/{path}", status_code=HTTP_204_NO_CONTENT)
async def drop_metadata_indexes(
    path, dal: DataAccessLayer = Depends(get_data_access_layer)
):
    """Drop the database index on the given metadata key path."""
    try:
        await dal.drop_metadata_index(path)
    except ProgrammingError:
        raise HTTPException(HTTP_404_NOT_FOUND, f"Not found: {path}")


def init_app(app):
    app.include_router(mod, tags=["Index"], dependencies=[Depends(admin_required)])
