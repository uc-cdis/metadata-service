from collections.abc import Iterable
from enum import Enum

from authutils.token.fastapi import access_token
from asyncpg import UniqueViolationError
from fastapi import HTTPException, APIRouter, Security, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import httpx
from pydantic import BaseModel
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.status import (
    HTTP_200_OK,
    HTTP_201_CREATED,
    HTTP_409_CONFLICT,
    HTTP_400_BAD_REQUEST,
    HTTP_401_UNAUTHORIZED,
    HTTP_403_FORBIDDEN,
    HTTP_404_NOT_FOUND,
    HTTP_500_INTERNAL_SERVER_ERROR,
)

from . import config, logger
from .models import Metadata
from .query import search_metadata, get_metadata

mod = APIRouter()

# auto_error=False prevents FastAPI from raises a 403 when the request is missing
# an Authorization header. Instead, we want to return a 401 to signify that we did
# not recieve valid credentials
bearer = HTTPBearer(auto_error=False)


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


class CreateObjForIdInput(BaseModel):
    """
    Create object.

    file_name (str): Name for the file being uploaded
    aliases (list, optional): unique name to allow using in place of whatever GUID gets
        created for this upload
    metadata (dict, optional): any additional metadata to attach to the upload
    """

    file_name: str
    aliases: list = None
    metadata: dict = None


@mod.post("/objects")
async def create_object(
    body: CreateObjInput,
    request: Request,
    token: HTTPAuthorizationCredentials = Security(bearer),
):
    """
    Create object placeholder and attach metadata, return Upload url to the user.

    Args:
        body (CreateObjInput): input body for create object
        request (Request): starlette request (which contains reference to FastAPI app)
        token (HTTPAuthorizationCredentials, optional): bearer token
    """
    try:
        # NOTE: token can be None if no Authorization header was provided, we expect
        #       this to cause a downstream exception since it is invalid
        token_claims = await access_token("user", "openid", purpose="access")(token)
    except Exception as exc:
        logger.error(exc, exc_info=True)
        raise HTTPException(
            HTTP_401_UNAUTHORIZED,
            f"Could not verify, parse, and/or validate scope from provided access token.",
        )

    file_name = body.file_name
    authz = body.authz
    aliases = body.aliases or []
    metadata = body.metadata

    logger.debug(f"validating authz block input: {authz}")
    if not _is_authz_version_supported(authz):
        raise HTTPException(HTTP_400_BAD_REQUEST, f"Unsupported authz version: {authz}")

    logger.debug(f"validating authz.resource_paths: {authz.get('resource_paths')}")
    if not isinstance(authz.get("resource_paths"), Iterable):
        raise HTTPException(
            HTTP_400_BAD_REQUEST,
            f"Invalid authz.resource_paths, must be valid list of resources, got: {authz.get('resource_paths')}",
        )

    metadata = metadata or {}

    # get user id from token claims
    uploader = token_claims.get("sub")
    auth_header = str(request.headers.get("Authorization", ""))

    blank_guid, signed_upload_url = await _create_blank_record_and_url(
        file_name, authz, auth_header, request
    )

    if aliases:
        await _create_aliases_for_record(aliases, blank_guid, auth_header, request)

    metadata = await _add_metadata(blank_guid, metadata, authz, uploader)

    response = {
        "guid": blank_guid,
        "aliases": aliases,
        "metadata": metadata,
        "upload_url": signed_upload_url,
    }

    return JSONResponse(response, HTTP_201_CREATED)


@mod.post("/objects/{guid:path}")
async def create_object_for_id(
    guid: str,
    body: CreateObjForIdInput,
    request: Request,
    token: HTTPAuthorizationCredentials = Security(bearer),
) -> JSONResponse:
    """
    Create object placeholder and attach metadata, return Upload url to the
    user. A new GUID (new version of the provided GUID) will be created for
    this object. The new record will have the same authz as the original one.

    Args:
        guid (str): indexd GUID or alias
        body (CreateObjForIdInput): input body for create object for ID
        request (Request): starlette request (which contains reference to FastAPI app)
        token (HTTPAuthorizationCredentials, optional): bearer token
    """
    try:
        # NOTE: token can be None if no Authorization header was provided, we expect
        #       this to cause a downstream exception since it is invalid
        token_claims = await access_token("user", "openid", purpose="access")(token)
    except Exception as exc:
        logger.error(exc, exc_info=True)
        raise HTTPException(
            HTTP_401_UNAUTHORIZED,
            "Could not verify, parse, and/or validate scope from provided access token.",
        )

    # hit indexd's GUID/alias resolution endpoint to get the indexd did
    try:
        endpoint = config.INDEXING_SERVICE_ENDPOINT.rstrip("/") + f"/{guid}"
        response = await request.app.async_client.get(endpoint)
        response.raise_for_status()

        # if the object is found in indexd, we can proceed
        indexd_record = response.json()
        indexd_did = indexd_record["did"]
    except httpx.HTTPError as err:
        logger.debug(err)
        if err.response and err.response.status_code == 404:
            msg = f"Could not find GUID or alias '{guid}' in indexd"
            logger.debug(msg)
            raise HTTPException(
                HTTP_404_NOT_FOUND,
                msg,
            )
        else:
            msg = f"Unable to query indexd for GUID or alias '{guid}'"
            logger.error(f"{msg}\nException:\n{err}", exc_info=True)
            raise

    file_name = body.file_name
    aliases = body.aliases or []
    metadata = body.metadata

    metadata = metadata or {}

    # get user id from token claims
    uploader = token_claims.get("sub")
    auth_header = str(request.headers.get("Authorization", ""))

    # create a new version (blank record) of this indexd object
    new_version_did = await _create_blank_version(
        indexd_record, file_name, auth_header, request
    )
    logger.debug(f"Created a new version of {indexd_did}: {new_version_did}")

    # get an upload URL for the newly created blank record
    signed_upload_url = await _create_url_for_blank_record(
        new_version_did, auth_header, request
    )

    if aliases:
        await _create_aliases_for_record(aliases, new_version_did, auth_header, request)

    metadata = await _add_metadata(
        new_version_did, metadata, {"resource_paths": indexd_record["authz"]}, uploader
    )

    response = {
        "guid": new_version_did,
        "aliases": aliases,
        "metadata": metadata,
        "upload_url": signed_upload_url,
    }

    return JSONResponse(response, HTTP_201_CREATED)


@mod.get("/objects")
async def get_objects(
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
    _request_paths: str = Query(
        "*", description="Return results at this given offset."
    ),
) -> JSONResponse:
    """
    XXX comments
    """
    #  XXX just pass kwargs?
    metadata_objects = await search_metadata(
        request, data=data, limit=limit, offset=offset
    )

    try:
        endpoint = config.INDEXING_SERVICE_ENDPOINT.rstrip("/") + f"/index"
        response = await request.app.async_client.get(
            endpoint, params=request.query_params
        )
        response.raise_for_status()

        records = response.json()
    except httpx.HTTPError as err:
        logger.debug(err)
        if err.response and err.response.status_code == 404:
            #  XXX better error message
            msg = f"Got a 404 from indexd's /index endpoint :("
            logger.debug(msg)
            raise HTTPException(
                HTTP_404_NOT_FOUND,
                msg,
            )
        else:
            #  XXX better message
            msg = f"Unable to get successful response from indexd's /index endpoint"
            logger.error(f"{msg}\nException:\n{err}", exc_info=True)
            raise
    records = {r["did"]: r for r in records["records"]}

    #  import pdb; pdb.set_trace()
    response = {
        guid: {"record": records[guid], "metadata": o}
        for guid, o in metadata_objects.items()
        if guid in records
    }
    return response


@mod.get("/objects/{guid:path}/download")
async def get_object_signed_download_url(
    guid: str,
    request: Request,
) -> JSONResponse:
    """
    Send a GET request to the data access service to generate a signed download
    url for the given GUID or alias. Returns the generated signed download url
    to the user.

    Args:
        guid (str): indexd GUID or alias
        request (Request): starlette request (which contains reference to FastAPI app)

    Returns:
        200: { "url": signed download url }
        404: if the data access service can not find GUID/alias in indexd
        403: if the data access service returns a 401 or a 403
        500: if there is an error making the request to the data access service
        or the data access service returns any other 400-range or 500-range
        error
    """
    try:
        endpoint = (
            config.DATA_ACCESS_SERVICE_ENDPOINT.rstrip("/") + f"/data/download/{guid}"
        )
        auth_header = str(request.headers.get("Authorization", ""))
        logger.debug(
            f"Attempting to GET signed download url from data access service for GUID or alias '{guid}'"
        )
        response = await request.app.async_client.get(
            endpoint, headers={"Authorization": auth_header}
        )
        response.raise_for_status()
        signed_download_url = response.json().get("url")
    except httpx.HTTPError as err:
        msg = f"Unable to get signed download url from data access service for GUID or alias '{guid}'"
        logger.error(f"{msg}\nException:\n{err}", exc_info=True)
        if err.response:
            logger.error(
                f"data access service response status code: {err.response.status_code}\n"
                f"data access service response text:\n{getattr(err.response, 'text')}"
            )
            if err.response.status_code in (401, 403):
                raise HTTPException(
                    HTTP_403_FORBIDDEN,
                    f"{msg}. You do not have access to generate the download url for GUID or alias '{guid}'.",
                )
            elif err.response.status_code == 404:
                raise HTTPException(
                    HTTP_404_NOT_FOUND,
                    f"{msg}. Record with GUID or alias '{guid}' was not found in indexd.",
                )
        raise HTTPException(HTTP_500_INTERNAL_SERVER_ERROR, msg)

    response = {"url": signed_download_url}
    return JSONResponse(response, HTTP_200_OK)


@mod.get("/objects/{guid:path}/latest")
async def get_object_latest(guid: str, request: Request) -> JSONResponse:
    """
    Attempt to fetch the latest version of the provided guid/key from indexd.
    If the provided guid/key is found in indexd, return the indexd record and
    metadata object associated with the latest guid fetched from indexd. If the
    provided guid/key is not found in indexd, return the metadata object
    associated with the provided guid/key.

    Args:
        guid (str): indexd GUID or MDS key. alias is NOT supported because the
            corresponding endpoint in indexd does not accept alias
        request (Request): starlette request (which contains reference to FastAPI app)

    Returns:
        200: { "record": { indexd record }, "metadata": { MDS metadata } }
        404: if the key is not in indexd and not in MDS
    """
    mds_key = guid
    indexd_record = {}

    try:
        endpoint = (
            config.INDEXING_SERVICE_ENDPOINT.rstrip("/") + f"/index/{guid}/latest"
        )
        response = await request.app.async_client.get(endpoint)
        response.raise_for_status()

        # if the object is found in indexd, use the indexd did as MDS key
        indexd_record = response.json()
        mds_key = indexd_record["did"]
    except httpx.HTTPError as err:
        logger.debug(err)
        if err.response and err.response.status_code == 404:
            logger.debug(f"Could not find latest record for GUID '{guid}' in indexd")
        else:
            msg = f"Unable to query indexd for latest record for GUID '{guid}'"
            logger.error(f"{msg}\nException:\n{err}", exc_info=True)

    mds_metadata = await _get_metadata(mds_key)

    if not indexd_record and not mds_metadata:
        raise HTTPException(HTTP_404_NOT_FOUND, f"Not found: '{guid}'")

    response = {
        "record": indexd_record,
        "metadata": mds_metadata,
    }

    return JSONResponse(response, HTTP_200_OK)


@mod.get("/objects/{guid:path}")
async def get_object(guid: str, request: Request) -> JSONResponse:
    """
    Get the metadata associated with the provided key. If the key is an
    indexd GUID or alias, also returns the indexd record.

    Args:
        guid (str): indexd GUID or alias, or MDS key
        request (Request): starlette request (which contains reference to FastAPI app)

    Returns:
        200: { "record": { indexd record }, "metadata": { MDS metadata } }
        404: if the key is not in indexd and not in MDS
    """
    mds_key = guid

    # hit indexd's GUID/alias resolution endpoint to get the indexd did
    indexd_record = {}
    try:
        endpoint = config.INDEXING_SERVICE_ENDPOINT.rstrip("/") + f"/{guid}"
        response = await request.app.async_client.get(endpoint)
        response.raise_for_status()

        # if the object is found in indexd, use the indexd did as MDS key
        indexd_record = response.json()
        mds_key = indexd_record["did"]
    except httpx.HTTPError as err:
        logger.debug(err)
        if err.response and err.response.status_code == 404:
            logger.debug(f"Could not find GUID or alias '{guid}' in indexd")
        else:
            msg = f"Unable to query indexd for GUID or alias '{guid}'"
            logger.error(f"{msg}\nException:\n{err}", exc_info=True)

    mds_metadata = await _get_metadata(mds_key)

    if not indexd_record and not mds_metadata:
        raise HTTPException(HTTP_404_NOT_FOUND, f"Not found: '{guid}'")

    response = {
        "record": indexd_record,
        "metadata": mds_metadata,
    }

    return JSONResponse(response, HTTP_200_OK)


async def _get_metadata(mds_key: str) -> dict:
    """
    Query the metadata database for mds_key.

    Args:
        mds_key (str): key used to query the metadata database

    Returns:
        dict: the object queried from the metadata database
    """
    mds_metadata = {}
    try:
        logger.debug(f"Querying the metadata database directly for key '{mds_key}'")
        mds_metadata = await get_metadata(mds_key)
    except HTTPException as err:
        logger.debug(err)
        if err.status_code == 404:
            logger.debug(f"Could not find key '{mds_key}', returning empty metadata")
        else:
            msg = f"Unable to query key '{mds_key}', returning empty metadata"
            logger.error(f"{msg}\nException:\n{err}", exc_info=True)
    return mds_metadata


async def _create_aliases_for_record(
    aliases: list, blank_guid: str, auth_header: str, request: Request
):
    aliases_data = {"aliases": [{"value": alias} for alias in aliases]}
    logger.debug(f"trying to create aliases: {aliases_data}")
    try:
        endpoint = (
            config.INDEXING_SERVICE_ENDPOINT.rstrip("/")
            + f"/index/{blank_guid}/aliases"
        )

        # pass along the authorization header to indexd request
        headers = {"Authorization": auth_header}
        response = await request.app.async_client.post(
            endpoint, json=aliases_data, headers=headers
        )
        response.raise_for_status()
    except httpx.HTTPError as err:
        # check if user has permission for resources specified
        if err.response and err.response.status_code in (401, 403):
            logger.error(
                f"Creating aliases in indexd for guid {blank_guid} failed, status code: {err.response.status_code}. Response text: {getattr(err.response, 'text')}"
            )
            raise HTTPException(
                HTTP_403_FORBIDDEN,
                "You do not have access to create the aliases you are trying to assign: "
                f"{aliases} to the guid {blank_guid}",
            )
        elif err.response and err.response.status_code == 409:
            logger.error(
                f"Creating aliases in indexd for guid {blank_guid} failed, status code: {err.response.status_code}. Response text: {getattr(err.response, 'text')}"
            )
            raise HTTPException(
                HTTP_409_CONFLICT,
                f"Some of the aliases you are trying to assign to guid {blank_guid} ({aliases}) already exist",
            )

        msg = (
            f"Unable to create aliases for guid {blank_guid} "
            f"with aliases: {aliases}."
        )
        logger.error(f"{msg}\nException:\n{err}", exc_info=True)
        raise HTTPException(HTTP_500_INTERNAL_SERVER_ERROR, msg)

    logger.info(f"added aliases: {aliases} for guid: {blank_guid}")


async def _create_blank_version(
    indexd_record: dict,
    file_name: str,
    auth_header: str,
    request: Request,
):
    """
    Create a new, blank version of the provided indexd object.
    """
    guid = indexd_record["did"]
    logger.debug(f"trying to create a new blank record version for {guid}")
    try:
        endpoint = config.INDEXING_SERVICE_ENDPOINT.rstrip("/") + f"/index/blank/{guid}"
        data = {
            "file_name": file_name,
            # set authz, but do not set deprecated acl field:
            "authz": indexd_record["authz"],
        }
        headers = {"Authorization": auth_header}
        response = await request.app.async_client.post(
            endpoint, json=data, headers=headers
        )
        response.raise_for_status()

        indexd_record = response.json()
        new_version_did = indexd_record["did"]
    except httpx.HTTPError as err:
        # check if user has permission for resources specified
        if err.response and err.response.status_code in (401, 403):
            logger.error(
                f"Unable to create a blank version of record '{guid}', status code: "
                f"{err.response.status_code}. Response text: {getattr(err.response, 'text')}"
            )
            raise HTTPException(
                HTTP_403_FORBIDDEN,
                f"You do not have access to create a blank version of record of record '{guid}'",
            )
        elif err.response and err.response.status_code == 404:
            msg = f"Could not find GUID '{guid}' in indexd"
            logger.debug(msg)
            raise HTTPException(
                HTTP_404_NOT_FOUND,
                msg,
            )
        else:
            msg = f"Unable to create a blank version of record '{guid}'"
            logger.error(f"{msg}\nException:\n{err}", exc_info=True)
            raise
    return new_version_did


async def _create_blank_record_and_url(
    file_name: str, authz: dict, auth_header: str, request: Request
):
    blank_guid = None
    signed_upload_url = None

    blank_record_params = {
        "file_name": file_name,
        "authz": authz.get("resource_paths", []),
    }

    logger.debug(
        f"trying to create blank record and generate an upload url using "
        f"params: {blank_record_params}"
    )
    try:
        endpoint = config.DATA_ACCESS_SERVICE_ENDPOINT.rstrip("/") + f"/data/upload"
        # pass along the authorization header to new request
        headers = {"Authorization": auth_header}
        response = await request.app.async_client.post(
            endpoint, json=blank_record_params, headers=headers
        )
        logger.debug(response)
        response.raise_for_status()
        blank_guid = response.json().get("guid")
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
                HTTP_403_FORBIDDEN,
                "You do not have access to generate the upload url for: "
                f"{blank_record_params}",
            )

        msg = (
            f"Unable to create signed upload url for guid {blank_guid} with params: "
            f"{blank_record_params}."
        )
        logger.error(f"{msg}\nException:\n{err}", exc_info=True)
        raise HTTPException(HTTP_500_INTERNAL_SERVER_ERROR, msg)

    return blank_guid, signed_upload_url


async def _create_url_for_blank_record(guid: str, auth_header: str, request: Request):
    logger.debug(f"trying to generate an upload url for {guid}")
    try:
        endpoint = (
            config.DATA_ACCESS_SERVICE_ENDPOINT.rstrip("/") + f"/data/upload/{guid}"
        )
        # pass along the authorization header to new request
        headers = {"Authorization": auth_header}
        response = await request.app.async_client.get(endpoint, headers=headers)
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
                HTTP_403_FORBIDDEN,
                f"You do not have access to generate the upload url for {guid}",
            )

        msg = f"Unable to create signed upload url for guid {guid}."
        logger.error(f"{msg}\nException:\n{err}", exc_info=True)
        raise HTTPException(HTTP_500_INTERNAL_SERVER_ERROR, msg)

    return signed_upload_url


async def _add_metadata(blank_guid: str, metadata: dict, authz: dict, uploader: str):
    # add default metadata to db
    additional_object_metadata = {
        "_resource_paths": authz.get("resource_paths", []),
        "_uploader_id": uploader,
        "_upload_status": FileUploadStatus.NOT_STARTED,
    }
    logger.debug(f"attempting to update guid {blank_guid} with metadata: {metadata}")
    metadata.update(additional_object_metadata)

    try:
        rv = (
            await Metadata.insert()
            .values(guid=blank_guid, data=metadata)
            .returning(*Metadata)
            .gino.first()
        )
    except UniqueViolationError:
        raise HTTPException(HTTP_409_CONFLICT, f"Metadata GUID conflict: {blank_guid}")

    return rv["data"]


def _is_authz_version_supported(authz):
    return str(authz.get("version", "")) == "0"


def init_app(app):
    app.include_router(mod, tags=["Object"])
