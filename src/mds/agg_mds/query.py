from fastapi import HTTPException, Path, Query, APIRouter, Request
from starlette.status import HTTP_404_NOT_FOUND
from mds import config
from mds.agg_mds import datastore
from typing import Any, Dict, List
from pydantic import BaseModel

mod = APIRouter()


@mod.get("/aggregate/commons")
async def get_commons():
    """Returns a list of all commons with data in the aggregate metadata-service

    Example:

        { commons: ["commonsA", "commonsB" ] }

    """
    return await datastore.get_commons()


@mod.get("/aggregate/info/{what}")
async def get_commons_info(what: str):
    """Returns status and configuration information about aggregate metadata service.

    Return configuration information. Currently supports only 1 information type:
    **schema**

    Example:

    {
        "__manifest":{
            "type":"array",
            "properties":{
                "file_name":{
                    "type":"string",
                    "description":""
                },
                "file_size":{
                    "type":"integer",
                    "description":""
                }
            },
            "description":"",
            "default":[

            ]
        },
        "commons_url":{
            "type":"string",
            "description":""
        },
        ...
    }

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
async def get_aggregate_metadata(
    _: Request,
    limit: int = Query(
        20, description="Maximum number of records returned. (e.g. max: 2000)"
    ),
    offset: int = Query(0, description="Return results at this given offset."),
    counts: str = Query(
        "",
        description="Return count of a field instead of the value if field is an array\
           otherwise field is unchanged. If field is **null** will set field to **0**.\
           Multiple fields can be compressed by comma separating the field names:\
           **files,authors**",
    ),
    flatten: bool = Query(
        False, description="Return the results without grouping items by commons."
    ),
    pagination: bool = Query(
        False, description="If true will return a pagination object in the response"
    ),
):
    """Returns metadata records

    Returns medata records namespaced by commons as a JSON object.
    Example without pagination:

        {
          "commonA" : {
              ... Metadata
          },
           "commonB" : {
              ... Metadata
          }
          ...
        }

    The pagination option adds a pagination object to the response:

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
    All other types will be unchanged.
    """
    results = await datastore.get_all_metadata(limit, offset, counts, flatten)
    if pagination is False:
        return results.get("results", {})
    return results


@mod.get("/aggregate/metadata/{name}")
async def get_aggregate_metadata_for_commons(
    name: str = Path(
        description="Return the results without grouping items by commons."
    ),
):
    """et all metadata records from a commons by name

    Returns an array containing all the metadata entries for a single commons.
    There are no limit/offset parameters.

    Example:

        [
            {
                "gen3_discovery": {
                    "name": "bear",
                    "type": "study",
                    ...
                },
                "data_dictionaries": {
                    ...
                }
            },
            {
                "gen3_discovery": {
                    "name": "cat",
                    "type": "study",
                    ...
                }
            },
            ...
        ]

    """
    res = await datastore.get_all_named_commons_metadata(name)
    if res:
        return res
    else:
        raise HTTPException(
            HTTP_404_NOT_FOUND,
            {"message": f"no common exists with the given: {name}", "code": 404},
        )


@mod.get("/aggregate/tags")
async def get_aggregate_tags():
    """Returns aggregate category, name and counts across all commons

    Example:

            {
              "Data Type": {
                "total": 275,
                "names": [
                  {
                    "Genotype": 103,
                    "Clinical Phenotype": 100,
                    "DCC Harmonized": 24,
                    "WGS": 20,
                    "SNP/CNV Genotypes (NGS)": 6,
                    "RNA-Seq": 5,
                    "WXS": 5,
                    "Targeted-Capture": 3,
                    "miRNA-Seq": 3,
                    "CNV Genotypes": 2
                  }
                ]
              }
            }
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
async def get_aggregate_metadata_commons_info(name: str):
    """
    Returns information from the named commons.

    Example:

        { commons_url: "gen3.datacommons.io" }

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
async def get_aggregate_metadata_guid(guid: str):
    """Returns a metadata record by GUID

    Example:

         {
            "gen3_discovery": {
                "name": "cat",
                "type": "study",
                ...
            }
        }
    """
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
        app.include_router(mod, tags=["Aggregate"])
