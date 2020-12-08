import operator

from fastapi import HTTPException, Query, APIRouter
from pydantic import Json
from sqlalchemy.sql import column
from starlette.requests import Request
from starlette.status import HTTP_404_NOT_FOUND

from .models import db, Metadata

mod = APIRouter()


@mod.get("/metadata")
#  XXX fix tests
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
):
    """
    XXX comments
    """
    #  XXX return await
    response = await search_metadata_helper(
        request.query_params, data=data, limit=limit, offset=offset
    )
    return response


#  XXX rename/update docstring
async def search_metadata_helper(
    #  request: Request,
    #  query_params: dict,
    data: bool = Query(
        False,
        description="Switch to returning a list of GUIDs (false), "
        "or GUIDs mapping to their metadata (true).",
    ),
    limit: int = Query(
        10, description="Maximum number of records returned. (max: 2000)"
    ),
    offset: int = Query(0, description="Return results at this given offset."),
    #  XXX description
    filter: Json = Query(Json(), description="The filters!"),
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
    #  queries = {}
    #  for key, value in request.query_params.multi_items():
    #  for key, value in query_params.items():
    #  if key not in {"data", "limit", "offset"}:
    #  queries.setdefault(key, []).append(value)
    filter_operators = {"eq": operator.eq, "in": operator.contains}

    #  XXX should maybe default query
    def add_filter(query):
        #  for path, values in queries.items():
        #  query = query.where(
        #  db.or_(Metadata.data[list(path.split("."))].astext == v for v in values)
        #  )
        for _filter in filter:
            if _filter["op"] == "eq":
                query = query.where(
                    Metadata.data[list(_filter["name"].split("."))].astext
                    == _filter["val"]
                )
            elif _filter["op"] == "has":
                query = query.where(
                    Metadata.data[list(_filter["name"].split("."))].has_key(
                        _filter["val"]
                    )
                    #  XXX from sqlalchemy for line below: NotImplementedError: Operator 'contains' is not supported on this expression
                    #  _filter['val'] in Metadata.data[list(_filter['name'].split("."))]
                )
            elif _filter["op"] == "any":
                array_func = db.func.jsonb_array_elements_text(
                    Metadata.data[list(_filter["name"].split("."))]
                ).alias()
                query = (
                    db.select(Metadata)
                    .select_from(Metadata)
                    .select_from(array_func)
                    .where(column(array_func.name).like(_filter["val"]))
                )
        return query.offset(offset).limit(limit)

    if data:
        return {
            metadata.guid: metadata.data
            for metadata in await add_filter(Metadata.query).gino.all()
        }
    else:
        return [
            row[0]
            for row in await add_filter(db.select([Metadata.guid]))
            .gino.return_model(False)
            .all()
        ]


@mod.get("/metadata/{guid:path}")
async def get_metadata(guid):
    """Get the metadata of the GUID."""
    metadata = await Metadata.get(guid)
    if metadata:
        return metadata.data
    else:
        raise HTTPException(HTTP_404_NOT_FOUND, f"Not found: {guid}")


def init_app(app):
    app.include_router(mod, tags=["Query"])
