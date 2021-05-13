from fastapi import HTTPException, Query, APIRouter, Request
from starlette.status import HTTP_404_NOT_FOUND
from mds.agg_mds.redis_cache import redis_cache
from mds import config

mod = APIRouter()


@mod.get("/aggregate/commons")
async def get_commons():
    """
    Returns a list of all registered commons
    :return:
    """
    return await redis_cache.get_commons()


@mod.get("/aggregate/metadata")
async def metadata(
    _: Request,
    limit: int = Query(
        20, description="Maximum number of records returned. (max: 2000)"
    ),
    offset: int = Query(0, description="Return results at this given offset."),
):
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

    return await redis_cache.get_all_metadata(limit, offset)


@mod.get("/aggregate/metadata/{name}")
async def metdata_name(name: str):
    """
    Returns the all the metadata from the named commons.
    """
    res = await redis_cache.get_all_named_commons_metadata(name)
    if res:
        return res
    else:
        raise HTTPException(
            HTTP_404_NOT_FOUND,
            {"message": f"no common exists with the given: {name}", "code": 404},
        )


@mod.get("/aggregate/metadata/{name}/tags")
async def metdata_tags(name: str):
    """
    Returns the tags associated with the named commons.
    """
    res = await redis_cache.get_commons_attribute(name, "tags")
    if res:
        return res
    else:
        raise HTTPException(
            HTTP_404_NOT_FOUND,
            {"message": f"no common exists with the given: {name}", "code": 404},
        )


@mod.get("/aggregate/metadata/{name}/info")
async def metdata_info(name: str):
    """
    Returns information from the named commons.
    """
    res = await redis_cache.get_commons_attribute(name, "info")
    if res:
        return res
    else:
        raise HTTPException(
            HTTP_404_NOT_FOUND,
            {"message": f"no common exists with the given: {name}", "code": 404},
        )


@mod.get("/aggregate/metadata/{name}/columns_to_fields")
async def metadata_columns_to_fields(name: str):
    res = await redis_cache.get_commons_attribute(name, "field_mapping")
    if res:
        return res
    else:
        raise HTTPException(
            HTTP_404_NOT_FOUND,
            {"message": f"no common exists with the given: {name}", "code": 404},
        )


@mod.get("/aggregate/metadata/{name}/aggregations")
async def metadtata_aggregations(name: str):
    res = await redis_cache.get_commons_attribute(name, "aggregations")
    if res:
        return res
    else:
        raise HTTPException(
            HTTP_404_NOT_FOUND,
            {"message": f"no common exists with the given: {name}", "code": 404},
        )


@mod.get("/aggregate/metadata/{name}/guid/{guid}:path")
async def metadata_name_guid(name: str, guid: str):
    """Get the metadata of the GUID in the named commons."""
    res = await redis_cache.get_commons_metadata_guid(name, guid)
    if res:
        return res
    else:
        raise HTTPException(
            HTTP_404_NOT_FOUND,
            {
                "message": f"no common/guid exists with the given: {name}/{guid}",
                "code": 404,
            },
        )


def init_app(app):
    if config.USE_AGG_MDS:
        app.include_router(mod, tags=["Query"])
