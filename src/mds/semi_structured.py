"""
Note: Must ensure that the /semi-structured endpoints in aliases.py load before those here.
I think it happens now because aliases.py is lexicographically sorted before semi_structured.py
"""
import uuid
import json
from asyncpg import UniqueViolationError
from fastapi import HTTPException, Query, APIRouter, Depends
import httpx
from starlette.responses import JSONResponse
from starlette.status import (
    HTTP_200_OK,
    HTTP_201_CREATED,
    HTTP_204_NO_CONTENT,
    HTTP_400_BAD_REQUEST,
    HTTP_404_NOT_FOUND,
    HTTP_409_CONFLICT,
    HTTP_500_INTERNAL_SERVER_ERROR,
)

from .admin_login import admin_required
from .models import Metadata, MetadataAlias
from . import config

mod = APIRouter()


@mod.get("/semi-structured/{guid:path}/versions")
async def get_semi_structured_data_versions(
    guid: str,
    data: bool = Query(
        False,
        description="Switch to returning a list of GUIDs (false), "
        "or GUIDs mapping to their metadata (true).",
    ),
) -> JSONResponse:
    """
    Get all versions of uniquely identified semi-structured data.

    Args:
        guid (str): GUID or alias
        data (bool): output full metadata else just GUIDs (default false)

    Returns:
        200: if successfully get versions
        404: if record with specified GUID not found
    """
    # resolve alias if exists
    alias_record = await MetadataAlias.get(guid)
    if alias_record:
        guid = alias_record.guid

    existing_record = await Metadata.get(guid)

    if not existing_record:
        raise HTTPException(HTTP_404_NOT_FOUND, f"Not found: {guid}")

    if existing_record.baseid:
        versions = await Metadata.query.where(
            Metadata.baseid == existing_record.baseid
        ).gino.all()
    else:
        versions = [existing_record]

    response = {"versions": []}
    for version in versions:
        output = {"guid": version.guid}
        if data:
            output |= {"guid_type": "semi-structured"}
            if version.created_date:
                output |= {"created_date": version.created_date.isoformat()}
            output |= version.data
        response["versions"].append(output)

    return JSONResponse(response, HTTP_200_OK)


@mod.get("/semi-structured/{guid:path}/latest")
async def get_semi_structured_data_latest(guid: str) -> JSONResponse:
    """
    Get latest version of uniquely identified semi-structured data.

    Args:
        guid (str): GUID or alias

    Returns:
        200: if successfully get latest version
        404: if record with specified GUID not found
    """
    # resolve alias if exists
    alias_record = await MetadataAlias.get(guid)
    if alias_record:
        guid = alias_record.guid

    existing_record = await Metadata.get(guid)

    if not existing_record:
        raise HTTPException(HTTP_404_NOT_FOUND, f"Not found: {guid}")

    if not existing_record.created_date:
        raise HTTPException(
            HTTP_400_BAD_REQUEST, f"Cannot call /latest on metadata record"
        )

    if existing_record.baseid:
        latest = (
            await Metadata.query.where(Metadata.baseid == existing_record.baseid)
            .order_by(Metadata.created_date.desc())
            .gino.first()
        )
    else:
        latest = existing_record

    response = {
        "guid": latest.guid,
        "guid_type": "semi-structured",
    }
    if latest.created_date:
        response |= {"created_date": latest.created_date.isoformat()}
    response |= latest.data

    return JSONResponse(response, HTTP_200_OK)


@mod.get("/semi-structured/{guid:path}")
async def get_semi_structured_data(guid: str) -> JSONResponse:
    """
    Get the semi-structured data record associated with the specified GUID or alias.

    Args:
        guid (str): GUID or alias

    Returns:
        200: { "guid": guid, "guid_type": "semi-structured", ...data... }
        404: if record with specified GUID not found
        500: if alias record exists but corresponding GUID not found
    """
    # resolve alias if exists
    alias_record = await MetadataAlias.get(guid)
    if alias_record:
        guid = alias_record.guid

    existing_record = await Metadata.get(guid)
    if not existing_record:
        raise HTTPException(HTTP_404_NOT_FOUND, f"Not found: {guid}")

    response = {"guid": guid}
    if existing_record.created_date:
        response |= {"guid_type": "semi-structured"}
    response |= existing_record.data
    return JSONResponse(response, HTTP_200_OK)


@mod.post("/semi-structured/{guid:path}", dependencies=[Depends(admin_required)])
async def create_semi_structured_data(guid: str, data: dict) -> JSONResponse:
    """
    Create a brand new record with the specified GUID or alias.

    Args:
        guid (str): GUID or alias
        data (dict): semi-structured data

    Returns:
        201: { "guid": guid, "guid_type": "semi-structured", ...data... }
        400: if invalid data, e.g. data specifies guid inconsistent with query parameter guid
        403: if authorization insufficient for requested action
        409: if record with specified GUID already exists
    """
    # resolve alias if exists
    alias_record = await MetadataAlias.get(guid)
    if alias_record:  # should cause 409
        guid = alias_record.guid

    # guid_type must be "semi-structured"
    if "guid_type" in data and data["guid_type"] != "semi-structured":
        raise HTTPException(
            HTTP_400_BAD_REQUEST,
            f"Query param data['guid_type'] must equal 'semi-structured'",
        )

    # guid must equal data['guid'], if it exists
    if "guid" in data and data["guid"] != guid:
        raise HTTPException(
            HTTP_400_BAD_REQUEST,
            f"Query param guid='{guid}' inconsistent with data['guid']='{data['guid']}'",
        )

    # data attribute should not contain "guid" or "guid_type" if exists
    data.pop("guid", None)
    data.pop("guid_type", None)

    try:
        record = (
            await Metadata.insert()
            .values(
                guid=guid,
                data=data,
                authz=json.loads(config.DEFAULT_AUTHZ_STR),
            )
            .returning(*Metadata)
            .gino.first()
        )
    except UniqueViolationError:
        raise HTTPException(HTTP_409_CONFLICT, f"Conflict: {guid}")

    response = {
        "guid": guid,
        "guid_type": "semi-structured",
    } | record["data"]

    return JSONResponse(response, HTTP_201_CREATED)


@mod.put("/semi-structured/{guid:path}", dependencies=[Depends(admin_required)])
async def update_semi_structured_data(guid: str, data: dict) -> JSONResponse:
    """
    Create new version of existing record with specified GUID or alias; if no existing record, then create brand new record. Either way, return the final object created which may or may not have the same GUID specified above.

    Args:
        guid (str): GUID or alias
        data (dict): semi-structured data

    Returns:
        201: { "guid": guid, "guid_type": "semi-structured", ...data... }
        400: if invalid data, e.g. data specifies guid_type other than "semi-structured" or data doesn't specify updated guid despite existing record
        403: if authorization insufficient for requested action
        409: if record with specified GUID already exists
        500: if data['guid'] matches alias but corresponding GUID not found
    """
    # resolve alias if exists
    alias_record = await MetadataAlias.get(guid)
    if alias_record:
        guid = alias_record.guid

    # guid_type must be "semi-structured"
    if "guid_type" in data and data["guid_type"] != "semi-structured":
        raise HTTPException(
            HTTP_400_BAD_REQUEST,
            f"Query param data['guid_type'] must equal 'semi-structured'",
        )

    existing_record = await Metadata.get(guid)

    # if specifying new_guid=data['guid'], then record with old_guid must already exist
    if "guid" in data and data["guid"] != guid and not existing_record:
        raise HTTPException(
            HTTP_400_BAD_REQUEST,
            f"Query param guid='{guid}' different from data['guid']='{data['guid']}' yet no record to update",
        )

    # if updating existing record, then data must specify the updated guid
    if existing_record and "guid" not in data:
        raise HTTPException(
            HTTP_400_BAD_REQUEST,
            f"Query param data doesn't specify updated guid",
        )

    # cannot update metadata record using this endpoint
    if existing_record and not existing_record.created_date:
        raise HTTPException(
            HTTP_400_BAD_REQUEST,
            f"Cannot update metadata record. Use /metadata endpoint instead",
        )

    # set baseid
    if existing_record:
        if existing_record.baseid:
            baseid = existing_record.baseid
        else:
            baseid = str(uuid.uuid4())
            await existing_record.update(baseid=baseid).apply()
    else:
        baseid = None

    # data attribute should not contain "guid" or "guid_type" if exists
    guid = data.pop("guid", guid)
    data.pop("guid_type", None)

    # resolve data['guid'] as alias if exists
    alias_record = await MetadataAlias.get(guid)
    if alias_record:  # should cause either 409 or 500 error
        guid = alias_record.guid

    try:
        record = (
            await Metadata.insert()
            .values(
                guid=guid,
                data=data,
                authz=json.loads(config.DEFAULT_AUTHZ_STR),
                baseid=baseid,
            )
            .returning(*Metadata)
            .gino.first()
        )
    except UniqueViolationError:
        raise HTTPException(HTTP_409_CONFLICT, f"Conflict: {guid}")

    # if data['guid'] matches existing alias but corresponding guid not found
    if alias_record:
        message = f"Alias record exists but GUID not found: {guid}"
        logging.error(message)
        raise HTTPException(HTTP_500_INTERNAL_SERVER_ERROR, message)

    response = {
        "guid": guid,
        "guid_type": "semi-structured",
    } | record["data"]

    return JSONResponse(response, HTTP_201_CREATED)


@mod.delete("/semi-structured/{guid:path}", dependencies=[Depends(admin_required)])
async def delete_semi_structured_data(guid: str) -> JSONResponse:
    """
    Delete the record with the specified GUID or alias permanently. If possible, AVOID deletion as it reduces reproducibility of research using this data.

    Args:
        guid (str): GUID or alias

    Returns:
        204: if record is successfully deleted
        403: if authorization insufficient for requested action
        404: if record with specified GUID not found
    """
    # resolve alias if exists
    alias_record = await MetadataAlias.get(guid)
    if alias_record:
        guid = alias_record.guid

    existing_record = (
        await Metadata.delete.where(Metadata.guid == guid)
        .where(Metadata.created_date != None)
        .returning(*Metadata)
        .gino.first()
    )
    if not existing_record:
        raise HTTPException(HTTP_404_NOT_FOUND, f"Not found: {guid}")

    return JSONResponse({}, HTTP_204_NO_CONTENT)


def init_app(app):
    app.include_router(mod, tags=["Semi-Structured"])
