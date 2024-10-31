from fastapi import HTTPException, Query, APIRouter
from starlette.requests import Request
from starlette.status import HTTP_404_NOT_FOUND, HTTP_500_INTERNAL_SERVER_ERROR
from starlette.responses import JSONResponse

from .models import db, Metadata, MetadataAlias
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

    def add_filter(query):
        for path, values in queries.items():
            if "*" in values:
                # query all records with a value for this path
                path = list(path.split("."))
                field = path.pop()
                query = query.where(Metadata.data[path].has_key(field))
            else:
                values = ["*" if v == "\*" else v for v in values]
                if "." in path:
                    path = list(path.split("."))
                query = query.where(
                    db.or_(Metadata.data[path].astext == v for v in values)
                )

        # TODO/FIXME: There's no updated date on the records, and without that
        # this "pagination" is prone to produce inconsistent results if someone is
        # trying to paginate using offset WHILE data is being added
        #
        # The only real way to try and reduce that risk
        # is to order by updated date (so newly added stuff is
        # at the end and new records don't end up in a page earlier on)
        # This is how our indexing service handles this situation.
        #
        # But until we have an updated_date, we can't do that, so naively order by
        # GUID for now and accept this inconsistency risk.
        query = query.order_by(Metadata.guid)

        query = query.offset(offset).limit(limit)
        return query

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


@mod.get("/metadata/{guid:path}/aliases")
async def get_metadata_aliases(
    guid: str,
) -> JSONResponse:
    """
    Get the aliases for the provided GUID

    Args:
        guid (str): Metadata GUID
    """
    metadata_aliases = await MetadataAlias.query.where(
        MetadataAlias.guid == guid
    ).gino.all()

    aliases = [metadata_alias.alias for metadata_alias in metadata_aliases]
    return {"guid": guid, "aliases": sorted(aliases)}


@mod.get("/metadata/{guid:path}")
async def get_metadata(guid):
    """Get the metadata of the GUID."""
    metadata = await Metadata.get(guid)

    if not metadata:
        # check if it's an alias
        alias = guid
        metadata_alias = await MetadataAlias.query.where(
            MetadataAlias.alias == alias
        ).gino.first()

        if not metadata_alias:
            raise HTTPException(HTTP_404_NOT_FOUND, f"Not found: {guid}")

        # get metadata for guid based on alias
        metadata = await Metadata.get(metadata_alias.guid)

        if not metadata:
            message = f"Alias record exists but GUID not found: {guid}"
            logging.error(message)
            raise HTTPException(HTTP_500_INTERNAL_SERVER_ERROR, message)

    return metadata.data


def init_app(app):
    app.include_router(mod, tags=["Query"])
