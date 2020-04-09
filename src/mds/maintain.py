import re

from asyncpg import UniqueViolationError
from fastapi import HTTPException, APIRouter, Depends
from sqlalchemy import bindparam
from sqlalchemy.dialects.postgresql import insert
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.status import HTTP_201_CREATED, HTTP_404_NOT_FOUND, HTTP_409_CONFLICT

from .admin_login import admin_required
from .models import db, Metadata

INDEX_REGEXP = re.compile(r"data #>> '{(.+)}'::text")

mod = APIRouter()


@mod.post("/metadata")
async def batch_create_metadata(
    request: Request, data_list: list, overwrite: bool = True
):
    """Create metadata in batch."""
    request.scope.get("add_close_watcher", lambda: None)()
    created = []
    updated = []
    conflict = []
    async with db.acquire() as conn:
        if overwrite:
            data = bindparam("data")
            stmt = await conn.prepare(
                insert(Metadata)
                .values(guid=bindparam("guid"), data=data)
                .on_conflict_do_update(
                    index_elements=[Metadata.guid], set_=dict(data=data)
                )
                .returning(db.text("xmax"))
            )
            for data in data_list:
                if await stmt.scalar(data) == 0:
                    created.append(data["guid"])
                else:
                    updated.append(data["guid"])
        else:
            stmt = await conn.prepare(
                insert(Metadata).values(guid=bindparam("guid"), data=bindparam("data"))
            )
            for data in data_list:
                try:
                    await stmt.status(data)
                except UniqueViolationError:
                    conflict.append(data["guid"])
                else:
                    created.append(data["guid"])
    return dict(created=created, updated=updated, conflict=conflict)


@mod.post("/metadata/{guid:path}")
async def create_metadata(guid, data: dict, overwrite: bool = False):
    """Create metadata for the GUID."""
    created = True
    if overwrite:
        rv = await db.first(
            insert(Metadata)
            .values(guid=guid, data=data)
            .on_conflict_do_update(index_elements=[Metadata.guid], set_=dict(data=data))
            .returning(Metadata.data, db.text("xmax"))
        )
        if rv["xmax"] != 0:
            created = False
    else:
        try:
            rv = (
                await Metadata.insert()
                .values(guid=guid, data=data)
                .returning(*Metadata)
                .gino.first()
            )
        except UniqueViolationError:
            raise HTTPException(HTTP_409_CONFLICT, f"Conflict: {guid}")
    if created:
        return JSONResponse(rv["data"], HTTP_201_CREATED)
    else:
        return rv["data"]


@mod.put("/metadata/{guid:path}")
async def update_metadata(guid, data: dict, merge: bool = False):
    """Update the metadata of the GUID.

    If `merge` is True, then any top-level keys that are not in the new data will be
    kept, and those that also exist in the new data will be replaced completely. This
    is also known as the shallow merge. The metadata service currently doesn't support
    deep merge.
    """
    metadata = (
        await Metadata.update.values(data=(Metadata.data + data) if merge else data)
        .where(Metadata.guid == guid)
        .returning(*Metadata)
        .gino.first()
    )
    if metadata:
        return metadata.data
    else:
        raise HTTPException(HTTP_404_NOT_FOUND, f"Not found: {guid}")


@mod.delete("/metadata/{guid:path}")
async def delete_metadata(guid):
    """Delete the metadata of the GUID."""
    metadata = (
        await Metadata.delete.where(Metadata.guid == guid)
        .returning(*Metadata)
        .gino.first()
    )
    if metadata:
        return metadata.data
    else:
        raise HTTPException(HTTP_404_NOT_FOUND, f"Not found: {guid}")


def init_app(app):
    app.include_router(mod, tags=["Maintain"], dependencies=[Depends(admin_required)])
