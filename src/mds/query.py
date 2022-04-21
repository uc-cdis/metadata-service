from typing import Optional
from fastapi import HTTPException, Query, APIRouter, Path
from starlette.requests import Request
from starlette.status import HTTP_404_NOT_FOUND, HTTP_500_INTERNAL_SERVER_ERROR

from .models import db, Metadata, MetadataInternal
from mds import config

mod = APIRouter()


@mod.get("/metadata")
async def search_metadata(
    request: Request,
    data: bool = Query(
        False,
        description="Switch to returning a list of GUIDs (false), "
        "or GUIDs mapping to their metadata (true).",
    ),
    limit: int = Query(
        10, description="Maximum number of records returned. (max: 2000)"
    ),
    offset: int = Query(0, description="Return results at this given offset."),
    internal_id: bool = Query(
        False,
        description="Switch to including the internal accession-style ID with data (true), or not (false). Only works when returning metadata",
    ),
):
    """Search the metadata.

    Without filters, this will return all data. Add filters as query strings like this:

        GET /metadata?a=1&b=2

    This will match all records that have metadata containing all of:

        {"a": 1, "b": 2}

    The values are always treated as strings for filtering. Nesting is supported:

        GET /metadata?a.b.c=3

    Matching records containing:

        {"a": {"b": {"c": 3}}}

    Providing the same key with more than one value filters records whose value of the
    given key matches any of the given values. But values of different keys must all
    match. For example:

        GET /metadata?a.b.c=3&a.b.c=33&a.b.d=4

    Matches these:

        {"a": {"b": {"c": 3, "d": 4}}}
        {"a": {"b": {"c": 33, "d": 4}}}
        {"a": {"b": {"c": "3", "d": 4, "e": 5}}}

    But won't match these:

        {"a": {"b": {"c": 3}}}
        {"a": {"b": {"c": 3, "d": 5}}}
        {"a": {"b": {"d": 5}}}
        {"a": {"b": {"c": "333", "d": 4}}}

    """
    limit = min(limit, 2000)
    queries = {}
    include_internal_id = internal_id
    for key, value in request.query_params.multi_items():
        if key not in {"data", "limit", "offset", "internal_id"}:
            if key == config.DB_GEN3_INTERNAL_ID_ALIAS:
                include_internal_id = True
            queries.setdefault(key, []).append(value)

    query = Metadata.query
    if include_internal_id:
        query = Metadata.outerjoin(MetadataInternal).select()

    def add_filter(query):
        for path, values in queries.items():
            if path == config.DB_GEN3_INTERNAL_ID_ALIAS:
                try:
                    query = query.where(
                        db.or_(MetadataInternal.id == int(v) for v in values)
                    )
                except ValueError as error:
                    raise HTTPException(
                        HTTP_500_INTERNAL_SERVER_ERROR,
                        f"Invalid internal ID value: {error}",
                    )
            else:
                query = query.where(
                    db.or_(
                        Metadata.data[list(path.split("."))].astext == v for v in values
                    )
                )
        return query.offset(offset).limit(limit)

    if data:
        if include_internal_id:
            return {
                metadata.guid: {
                    **metadata.data,
                    config.DB_GEN3_INTERNAL_ID_ALIAS: metadata.id,
                }
                for metadata in await add_filter(query).gino.all()
            }
        return {
            metadata.guid: metadata.data
            for metadata in await add_filter(query).gino.all()
        }
    else:
        return [
            row[0]
            for row in await add_filter(db.select([Metadata.guid]))
            .gino.return_model(False)
            .all()
        ]


@mod.get("/metadata/{guid:path}")
async def get_metadata(
    guid,
    internal_id: Optional[bool] = None,
):
    """Get the metadata of the GUID."""
    if internal_id:
        metadata = (
            await Metadata.outerjoin(MetadataInternal)
            .select()
            .where(Metadata.guid == guid)
            .gino.first()
        )
    else:
        metadata = await Metadata.get(guid)

    if metadata:
        if internal_id:
            return {**metadata.data, config.DB_GEN3_INTERNAL_ID_ALIAS: metadata.id}
        return metadata.data
    else:
        raise HTTPException(HTTP_404_NOT_FOUND, f"Not found: {guid}")


def init_app(app):
    app.include_router(mod, tags=["Query"])
