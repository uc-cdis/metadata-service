from collections import Iterable
from enum import Enum
import json
import sys
import traceback

from authutils.token.fastapi import access_token
from asyncpg import UniqueViolationError
from fastapi import Depends, HTTPException, Query, APIRouter
from gen3authz.client.arborist.async_client import ArboristClient
import httpx
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.status import (
    HTTP_201_CREATED,
    HTTP_404_NOT_FOUND,
    HTTP_409_CONFLICT,
    HTTP_400_BAD_REQUEST,
    HTTP_401_UNAUTHORIZED,
    HTTP_500_INTERNAL_SERVER_ERROR,
)
from pydantic import BaseModel

from . import config, logger
from .models import db, Metadata

mod = APIRouter()
arborist = ArboristClient()


class FileUploadStatus(str, Enum):
    NOT_STARTED = "not_uploaded"
    DONE = "uploaded"
    ERROR = "error"


class CreateObjInput(BaseModel):
    """
    Create object.

    file_name (str): Name for the file being uploaded
    authz (dict): authorization block with requirements for what's being uploaded
    aliases (list, optional): unique name to allow using in place of whatever GUID gets
        created for this upload
    metadata (dict, optional): any additional metadata to attach to the upload
    """

    file_name: str
    authz: dict
    aliases: list = None
    metadata: dict = None


@mod.post("/objects")
async def create_object(
    body: CreateObjInput,
    request: Request,
    token=Depends(access_token("user", "openid", purpose="access")),
):
    """
    Create object placeholder and attach metadata, return Upload url to the user.

    Args:
        body (CreateObjInput): input body for create object
    """
    file_name = body.dict().get("file_name")
    authz = body.dict().get("authz")
    aliases = body.dict().get("aliases")
    metadata = body.dict().get("metadata")
    logger.debug(f"validating authz block input: {authz}")

    if not _is_authz_version_supported(authz):
        raise HTTPException(HTTP_400_BAD_REQUEST, f"Unsupported authz version: {authz}")

    logger.debug(f"validated authz.resource_paths: {authz.get('resource_paths')}")

    if not isinstance(authz.get("resource_paths"), Iterable):
        raise HTTPException(
            HTTP_400_BAD_REQUEST,
            f"Invalid authz.resource_paths, must be valid list of resources, got: {authz.get('resource_paths')}",
        )

    metadata = metadata or {}

    # get token claims
    uploader = token.get("sub")

    # create indexd blank record
    blank_guid = None
    blank_record_data = {
        "uploader": uploader,
        "file_name": file_name,
        "authz": authz.get("resource_paths", []),
    }
    logger.debug(f"trying to create a blank indexd record: {blank_record_data}")
    try:
        async with httpx.AsyncClient() as client:
            endpoint = config.INDEXING_SERVICE_ENDPOINT.rstrip("/") + "/index/blank"

            # pass along the authorization header to indexd request
            headers = {"Authorization": str(request.headers.get("Authorization", ""))}
            response = await client.post(
                endpoint, json=blank_record_data, headers=headers
            )
            response.raise_for_status()
            blank_guid = response.json().get("did")
    except httpx.HTTPError as err:
        # check if user has permission for resources specified
        if err.response and err.response.status_code in (401, 403):
            logger.error(
                f"Creating a blank record in indexd failed, status code: "
                f"{err.response.status_code}. Response text: {getattr(err.response, 'text')}"
            )
            raise HTTPException(
                HTTP_401_UNAUTHORIZED,
                "You do not have create access to the authz resources you are trying to assign: "
                f"{authz.get('resource_paths')}",
            )

        msg = (
            f"Unable to create a blank indexd record for user {uploader} "
            f"with filename: {file_name}."
        )
        logger.error(f"{msg}\nException:\n{err}", exc_info=True)
        raise HTTPException(HTTP_500_INTERNAL_SERVER_ERROR, msg)

    logger.info(f"created a blank indexd record: {blank_guid}")

    # create aliases for GUID if aliases is provided
    if aliases:
        aliases_data = {"aliases": [{"value": alias} for alias in aliases]}
        logger.debug(f"trying to create aliases: {aliases_data}")
        try:
            async with httpx.AsyncClient() as client:
                endpoint = (
                    config.INDEXING_SERVICE_ENDPOINT.rstrip("/")
                    + f"/index/{blank_guid}/aliases"
                )

                # pass along the authorization header to indexd request
                headers = {
                    "Authorization": str(request.headers.get("Authorization", ""))
                }
                response = await client.post(
                    endpoint, json=aliases_data, headers=headers
                )
                response.raise_for_status()
        except httpx.HTTPError as err:
            # check if user has permission for resources specified
            if err.response and err.response.status_code in (401, 403):
                logger.error(
                    f"Creating aliases in indexd failed, status code: "
                    f"{err.response.status_code}. Response text: {getattr(err.response, 'text')}"
                )
                raise HTTPException(
                    HTTP_401_UNAUTHORIZED,
                    "You do not have access to create the aliases you are trying to assign: "
                    f"{aliases} to the guid {blank_guid}",
                )

            msg = (
                f"Unable to create alises for guid {blank_guid} by user {uploader} "
                f"with aliases: {aliases}."
            )
            logger.error(f"{msg}\nException:\n{err}", exc_info=True)
            raise HTTPException(HTTP_500_INTERNAL_SERVER_ERROR, msg)

        logger.info(f"added aliases: {aliases} for guid: {blank_guid}")

    # TODO request upload URL from fence for GUID
    signed_upload_url = None
    upload_url_params = {"file_id": blank_guid, "protocol": "s3"}
    logger.debug(f"trying to generate an upload url using params: {upload_url_params}")
    try:
        async with httpx.AsyncClient() as client:
            endpoint = (
                config.DATA_ACCESS_SERVICE_ENDPOINT.rstrip("/")
                + f"/data/upload/{blank_guid}"
            )

            # pass along the authorization header to indexd request
            headers = {"Authorization": str(request.headers.get("Authorization", ""))}
            response = await client.get(
                endpoint, params=upload_url_params, headers=headers
            )
            logger.debug(response)
            response.raise_for_status()
            signed_upload_url = response.json().get("url")
    except httpx.HTTPError as err:
        logger.debug(err)
        # check if user has permission for resources specified
        if err.response and err.response.status_code in (401, 403):
            logger.error(
                f"Generating upload url failed, status code: "
                f"{err.response.status_code}. Response text: {getattr(err.response, 'text')}"
            )
            raise HTTPException(
                HTTP_401_UNAUTHORIZED,
                "You do not have access to generate the upload url for: "
                f"{upload_url_params}",
            )

        msg = (
            f"Unable to create signed upload url for guid {blank_guid} with params: "
            f"{upload_url_params}."
        )
        logger.error(f"{msg}\nException:\n{err}", exc_info=True)
        raise HTTPException(HTTP_500_INTERNAL_SERVER_ERROR, msg)

    # TODO add default metadata to db (_resource_paths, _uploader_id, _upload_status)
    additional_object_metadata = {
        "_resource_paths": authz.get("resource_paths", []),
        "_uploader_id": uploader,
        "_upload_status": FileUploadStatus.NOT_STARTED,
    }
    metadata.update(additional_object_metadata)
    logger.debug(f"attempting to update guid {blank_guid} with metadata: {metadata}")

    try:
        rv = (
            await Metadata.insert()
            .values(guid=blank_guid, data=metadata)
            .returning(*Metadata)
            .gino.first()
        )
    except UniqueViolationError:
        raise HTTPException(HTTP_409_CONFLICT, f"Metadata GUID conflict: {blank_guid}")

    response = {
        "guid": blank_guid,
        "metadata": metadata,
        "upload_url": signed_upload_url,
    }

    return JSONResponse(response, HTTP_201_CREATED)


def _is_authz_version_supported(authz):
    return str(authz.get("version", "")) == "0"


def init_app(app):
    app.include_router(mod, tags=["Object"])
