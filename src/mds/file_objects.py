from collections.abc import Iterable
from enum import Enum

from authutils.token.fastapi import access_token
from asyncpg import UniqueViolationError
from fastapi import HTTPException, APIRouter, Depends, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import httpx
import jsonschema
from urllib.parse import urljoin
import uuid
from pydantic import BaseModel
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.status import (
    HTTP_200_OK,
    HTTP_201_CREATED,
    HTTP_204_NO_CONTENT,
    HTTP_409_CONFLICT,
    HTTP_400_BAD_REQUEST,
    HTTP_401_UNAUTHORIZED,
    HTTP_403_FORBIDDEN,
    HTTP_404_NOT_FOUND,
    HTTP_500_INTERNAL_SERVER_ERROR,
)

from . import config, logger
from .authz import Auth
from .indexd_schema import POST_RECORD_SCHEMA  # TODO rename/reorg that file
from .models import FileObject
from .query import get_metadata

mod = APIRouter()

bearer = HTTPBearer(auto_error=False)


@mod.get("/file_objects", status_code=HTTP_200_OK)
async def get_file_object() -> list:
    """
    TODO
    """
    requests = await FileObject.query.gino.all()
    return {"records": [r.to_dict() for r in requests]}


@mod.post("/file_objects")
async def create_file_object(
    body: dict,  # TODO class instead of POST_RECORD_SCHEMA?
    auth=Depends(Auth),
):
    """
    TODO
    """
    try:
        jsonschema.validate(body, POST_RECORD_SCHEMA)
    except jsonschema.ValidationError as err:
        raise HTTPException(HTTP_400_BAD_REQUEST, err)

    authz = body.get("authz", [])
    await auth.authorize("create", authz)

    did = body.get("did")
    form = body["form"]
    size = body["size"]
    urls = body["urls"]
    acl = body.get("acl", [])

    hashes = body["hashes"]
    file_name = body.get("file_name")
    metadata = body.get("metadata")
    urls_metadata = body.get("urls_metadata")
    version = body.get("version")
    baseid = body.get("baseid")
    uploader = body.get("uploader")

    did, rev, baseid = await add(
        form,
        did,
        size=size,
        file_name=file_name,
        metadata=metadata,
        urls_metadata=urls_metadata,
        version=version,
        urls=urls,
        acl=acl,
        authz=authz,
        hashes=hashes,
        baseid=baseid,
        uploader=uploader,
    )

    ret = {"did": did, "rev": rev, "baseid": baseid}

    return JSONResponse(ret, HTTP_201_CREATED)


async def add(
    form,
    did=None,
    size=None,
    file_name=None,
    metadata=None,
    urls_metadata=None,
    version=None,
    urls=None,
    acl=None,
    authz=None,
    hashes=None,
    baseid=None,
    uploader=None,
):
    """
    Creates a new record given size, urls, acl, authz, hashes, metadata,
    urls_metadata file name and version
    if did is provided, update the new record with the did otherwise create it
    """

    urls = urls or []
    acl = acl or []
    authz = authz or []
    hashes = hashes or {}
    metadata = metadata or {}
    urls_metadata = urls_metadata or {}

    if not did:
        did = str(uuid.uuid4())
        # TODO prefix logic
        # if self.config.get("PREPEND_PREFIX"):
        #     did = self.config["DEFAULT_PREFIX"] + did

    if not baseid:
        baseid = str(uuid.uuid4())

    data = {
        "did": did,
        "baseid": baseid,
        "rev": str(uuid.uuid4())[:8],
        "form": form,
        "size": size,
        "file_name": file_name,
        "version": version,
        "uploader": uploader,
        "urls": urls,
        "urls_metadata": urls_metadata,
        "acl": acl,
        "authz": authz,
        "hashes": hashes,
        "metadata": metadata,
    }

    try:
        res = (
            await FileObject.insert().values(**data).returning(*FileObject).gino.first()
        )
    except UniqueViolationError:
        raise HTTPException(HTTP_409_CONFLICT, f"did '{did}' already exists")

    return res["did"], res["rev"], res["baseid"]


def init_app(app):
    app.include_router(mod, tags=["FileObject"])
