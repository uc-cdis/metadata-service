from fastapi import HTTPException, Query, APIRouter, Request
from starlette.status import HTTP_404_NOT_FOUND
from mds import config
from mds.agg_mds import datastore


mod = APIRouter()


@mod.get("/aggregate/commons")
async def get_commons():
    """
    Returns a list of all registered commons
    :return:
    """
    return await datastore.get_commons()


@mod.get("/aggregate/metadata")
async def metadata(
    _: Request,
    limit: int = Query(
        20, description="Maximum number of records returned. (max: 2000)"
    ),
    offset: int = Query(0, description="Return results at this given offset."),
):
    # TODO WFH How to properly return this? We think grouping by MDS is probably
    # not ideal in reality. We already have commons_name in the results.
    """
    Returns all metadata from all registered commons in the form:
    {
      "commonA" : {
          ... Metadata
      },
       "commonB" : {
          ... Metadata
      }
      ...
    }
    """
    return await datastore.get_all_metadata(limit, offset)


@mod.get("/aggregate/metadata/{name}")
async def metadata_name(name: str):
    """
    Returns the all the metadata from the named commons.
    """
    res = await datastore.get_all_named_commons_metadata(name)
    if res:
        return res
    else:
        raise HTTPException(
            HTTP_404_NOT_FOUND,
            {"message": f"no common exists with the given: {name}", "code": 404},
        )


@mod.get("/aggregate/metadata/{name}/tags")
async def metadata_tags(name: str):
    """
    Returns the tags associated with the named commons.
    """
    res = await datastore.get_commons_attribute(name, "tags")
    if res:
        return res
    else:
        raise HTTPException(
            HTTP_404_NOT_FOUND,
            {"message": f"no common exists with the given: {name}", "code": 404},
        )


@mod.get("/aggregate/metadata/{name}/info")
async def metadata_info(name: str):
    """
    Returns information from the named commons.
    """
    res = await datastore.get_commons_attribute(name, "info")
    if res:
        return res
    else:
        raise HTTPException(
            HTTP_404_NOT_FOUND,
            {"message": f"no common exists with the given: {name}", "code": 404},
        )


@mod.get("/aggregate/metadata/{name}/aggregations")
async def metadata_aggregations(name: str):
    res = await datastore.get_aggregations(name)
    if res:
        return res
    else:
        raise HTTPException(
            HTTP_404_NOT_FOUND,
            {"message": f"no common exists with the given: {name}", "code": 404},
        )


@mod.get("/aggregate/metadata/guid/{guid:path}")
async def metadata_name_guid(guid: str):
    """Get the metadata of the GUID in the named commons."""
    res = await datastore.get_by_guid(guid)
    if res:
        return res
    else:
        raise HTTPException(
            HTTP_404_NOT_FOUND,
            {
                "message": f"no entry exists with the given guid: {guid}",
                "code": 404,
            },
        )


def init_app(app):
    if config.USE_AGG_MDS:
        app.include_router(mod, tags=["Query"])
