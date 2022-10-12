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


@mod.get("/aggregate/info/{what}")
async def get_commons(what: str):
    """
    Returns information from the aggregate metadata service.
    """
    res = await datastore.get_commons_attribute(what)
    if res:
        return res
    else:
        raise HTTPException(
            HTTP_404_NOT_FOUND,
            {"message": f"information for {what} not found", "code": 404},
        )


@mod.get("/aggregate/metadata")
async def metadata(
    _: Request,
    limit: int = Query(
        20, description="Maximum number of records returned. (e.g. max: 2000)"
    ),
    offset: int = Query(0, description="Return results at this given offset."),
    counts: str = Query(
        "", description="Return count of a field instead of the value."
    ),
    flatten: bool = Query(
        False, description="Return the results without grouping items by commons."
    ),
    pagination: bool = Query(
        False, description="If true will return a pagination object in the response"
    ),
):
    """Returns metadata records

    The pagination option adds a pagination object to the response:
    {
      "commonA" : {
          ... Metadata
      },
       "commonB" : {
          ... Metadata
      }
      ...
    }

        {
        results: {
          "commonA" : {
              ... Metadata
          },
           "commonB" : {
              ... Metadata
          }
          ...
        },
        "pagination": {
            "hits": 64,
            "offset": 0,
            "pageSize": 20,
            "pages": 4
        }
    }

    The flatten option removes the commons' namespace so all results are a child or results:
        results: {
              ... Metadata from commons A
              ... Metadata from commons B
          }
          ...
        },


    The counts options when applied to an array or dictionary will replace
    the field value with its length. If the field values is None it will replace it with 0.
    All other type will be unchanged.
    """
    results = await datastore.get_all_metadata(limit, offset, counts, flatten)
    if pagination is False:
        return results.get("results", {})
    return results


@mod.get("/aggregate/metadata/{name}")
async def metadata_name(name: str):
    """Returns the all the metadata from the named commons."""
    res = await datastore.get_all_named_commons_metadata(name)
    if res:
        return res
    else:
        raise HTTPException(
            HTTP_404_NOT_FOUND,
            {"message": f"no common exists with the given: {name}", "code": 404},
        )


@mod.get("/aggregate/tags")
async def metadata_tags():
    """
    Returns the tags associated with the named commons.
    """
    res = await datastore.get_all_tags()
    if res:
        return res
    else:
        raise HTTPException(
            HTTP_404_NOT_FOUND,
            {"message": f"error retrieving tags from service", "code": 404},
        )


@mod.get("/aggregate/metadata/{name}/info")
async def metadata_info(name: str):
    """
    Returns information from the named commons.
    """
    res = await datastore.get_commons_attribute(name)
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
