from fastapi import HTTPException, Query, APIRouter, Depends
from starlette.requests import Request
from starlette.status import HTTP_404_NOT_FOUND, HTTP_500_INTERNAL_SERVER_ERROR
from starlette.responses import JSONResponse

from .db import get_data_access_layer, DataAccessLayer
from . import config

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
    data_access_layer: DataAccessLayer = Depends(get_data_access_layer),
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

    To query all rows with a given key, regardless of value, use the "*" wildcard. For example:

        GET /metadata?a=* or GET /metadata?a.b=*

    Note that only a single asterisk is supported, not true wildcarding. For
    example: `?a=1.*` will only match the exact string `"1.*"`.

    To query rows with a value of `"*"` exactly, escape the asterisk. For example: `?a=\*`.
    """
    limit = min(limit, config.METADATA_QUERY_RESULTS_LIMIT)
    queries = {}
    for key, value in request.query_params.multi_items():
        if key not in {"data", "limit", "offset"}:
            queries.setdefault(key, []).append(value)

    result = await data_access_layer.search_metadata(
        filters=queries,
        limit=limit,
        offset=offset,
        return_data=data,
    )

    return result


@mod.get("/metadata/{guid:path}/aliases")
async def get_metadata_aliases(
    guid: str,
    data_access_layer: DataAccessLayer = Depends(get_data_access_layer),
) -> JSONResponse:
    """
    Get the aliases for the provided GUID

    Args:
        guid (str): Metadata GUID
    """
    aliases = await data_access_layer.get_aliases_for_guid(guid)
    return {"guid": guid, "aliases": sorted(aliases)}


@mod.get("/metadata/{guid:path}")
async def get_metadata(
    guid,
    data_access_layer: DataAccessLayer = Depends(get_data_access_layer),
):
    """Get the metadata of the GUID."""
    metadata = await data_access_layer.get_metadata(guid)

    if not metadata:
        # check if it's an alias
        alias = guid
        metadata_alias = await data_access_layer.get_alias(alias)

        if not metadata_alias:
            raise HTTPException(HTTP_404_NOT_FOUND, f"Not found: {guid}")

        # get metadata for guid based on alias
        metadata = await data_access_layer.get_metadata_by_alias(alias)

        if not metadata:
            message = f"Alias record exists but GUID not found: {guid}"
            raise HTTPException(HTTP_500_INTERNAL_SERVER_ERROR, message)

    return metadata["data"]


def init_app(app):
    app.include_router(mod, tags=["Query"])
