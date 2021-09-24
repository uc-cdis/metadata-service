from collections.abc import Iterable
from enum import Enum

from authutils.token.fastapi import access_token
from asyncpg import UniqueViolationError
from dateutil import parser
from fastapi import HTTPException, APIRouter, Depends, Query, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import httpx
import json
import jsonschema
import re
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


ACCEPTABLE_HASHES = {
    "md5": re.compile(r"^[0-9a-f]{32}$").match,
    "sha1": re.compile(r"^[0-9a-f]{40}$").match,
    "sha256": re.compile(r"^[0-9a-f]{64}$").match,
    "sha512": re.compile(r"^[0-9a-f]{128}$").match,
    "crc": re.compile(r"^[0-9a-f]{8}$").match,
    "etag": re.compile(r"^[0-9a-f]{32}(-\d+)?$").match,
}


def validate_hashes(**hashes):
    """
    Validate hashes against known and valid hashing algorithms.
    """
    if not all(h in ACCEPTABLE_HASHES for h in hashes):
        raise HTTPException(HTTP_400_BAD_REQUEST, "invalid hash types specified")

    if not all(ACCEPTABLE_HASHES[h](v) for h, v in hashes.items()):
        raise HTTPException(HTTP_400_BAD_REQUEST, "invalid hash values specified")


@mod.get("/index", status_code=HTTP_200_OK)
@mod.get("/index/", status_code=HTTP_200_OK)
@mod.get("/file_objects", status_code=HTTP_200_OK)
async def list_file_objects(
    limit: int = Query(None),
    start: int = Query(None),
    page: int = Query(None),
    ids: str = Query(None),
    size: int = Query(None),
    uploader: str = Query(None),
    urls: list = Query(None),  # TODO get list of values for same param?
    url: list = Query(None),
    file_name: str = Query(None),
    version: int = Query(None),
    hash: list = Query(None),
    metadata: list = Query(None),
    acl: str = Query(None),
    authz: str = Query(None),
    urls_metadata: str = Query(None),
    negate_params: str = Query(None),
    form: str = Query(None),
) -> list:
    """
    TODO
    """
    if url and not urls:
        # allow "url" for backwards compatibility
        urls = url

    if ids:
        ids = ids.split(",")
        if start is not None or limit is not None or page is not None:
            raise HTTPException(
                HTTP_400_BAD_REQUEST, "pagination is not supported when ids is provided"
            )
    try:
        limit = 100 if limit is None else int(limit)
    except ValueError:
        raise HTTPException(HTTP_400_BAD_REQUEST, "limit must be an integer")

    if limit < 0 or limit > 1024:
        raise HTTPException(HTTP_400_BAD_REQUEST, "limit must be between 0 and 1024")

    if page is not None:
        try:
            page = int(page)
        except ValueError as err:
            raise HTTPException(HTTP_400_BAD_REQUEST, "page must be an integer")

    try:
        size = size if size is None else int(size)
    except ValueError as err:
        raise HTTPException(HTTP_400_BAD_REQUEST, "size must be an integer")

    if size is not None and size < 0:
        raise HTTPException(HTTP_400_BAD_REQUEST, "size must be > 0")

    hashes = {h: v for h, v in (x.split(":", 1) for x in hash)} if hash else {}

    validate_hashes(**hashes)
    hashes = hashes if hashes else None

    metadata = (
        {k: v for k, v in (x.split(":", 1) for x in metadata)} if metadata else {}
    )

    if acl is not None:
        acl = [] if acl == "null" else acl.split(",")

    if authz is not None:
        authz = [] if authz == "null" else authz.split(",")

    if urls_metadata:
        try:
            urls_metadata = json.loads(urls_metadata)
        except ValueError:
            raise HTTPException(
                HTTP_400_BAD_REQUEST, "urls_metadata must be a valid json string"
            )

    if negate_params:
        try:
            negate_params = json.loads(negate_params)
        except ValueError:
            raise HTTPException(
                HTTP_400_BAD_REQUEST, "negate_params must be a valid json string"
            )

    if form == "bundle":
        pass
        # TODO implement below
    #     records = blueprint.index_driver.get_bundle_list(
    #         start=start, limit=limit, page=page
    #     )
    # elif form == "all":
    #     records = blueprint.index_driver.get_bundle_and_object_list(
    #         limit=limit,
    #         page=page,
    #         start=start,
    #         size=size,
    #         urls=urls,
    #         acl=acl,
    #         authz=authz,
    #         hashes=hashes,
    #         file_name=file_name,
    #         version=version,
    #         uploader=uploader,
    #         metadata=metadata,
    #         ids=ids,
    #         urls_metadata=urls_metadata,
    #         negate_params=negate_params,
    #     )
    else:
        records = await get_file_object_ids(
            start=start,
            limit=limit,
            page=page,
            size=size,
            file_name=file_name,
            version=version,
            urls=urls,
            acl=acl,
            authz=authz,
            hashes=hashes,
            uploader=uploader,
            ids=ids,
            metadata=metadata,
            urls_metadata=urls_metadata,
            negate_params=negate_params,
        )

    result = {
        "ids": ids,
        "records": records,
        "limit": limit,
        "start": start,
        "page": page,
        "size": size,
        "file_name": file_name,
        "version": version,
        "urls": urls,
        "acl": acl,
        "authz": authz,
        "hashes": hashes,
        "metadata": metadata,
    }

    return JSONResponse(result, HTTP_200_OK)


async def get_file_object_ids(
    limit=100,
    page=None,
    start=None,
    size=None,
    urls=None,
    acl=None,
    authz=None,
    hashes=None,
    file_name=None,
    version=None,
    uploader=None,
    metadata=None,
    ids=None,
    urls_metadata=None,
    negate_params=None,
):
    """
    Returns list of records stored by the backend.
    """
    query = FileObject.query

    # Enable joinedload on all relationships so that we won't have to
    # do a bunch of selects when we assemble our response.
    # query = query.options(
    #     joinedload(IndexRecord.urls).joinedload(IndexRecordUrl.url_metadata)
    # )
    # query = query.options(joinedload(IndexRecord.acl))
    # query = query.options(joinedload(IndexRecord.authz))
    # query = query.options(joinedload(IndexRecord.hashes))
    # query = query.options(joinedload(IndexRecord.index_metadata))
    # query = query.options(joinedload(IndexRecord.aliases))

    # query = query.where(
    #     db.or_(Metadata.data[list(path.split("."))].astext == v for v in values)
    # )

    if start is not None:
        query = query.where(FileObject.did > start)

    if size is not None:
        query = query.where(FileObject.size == size)

    if file_name is not None:
        query = query.where(FileObject.file_name == file_name)

    if version is not None:
        query = query.where(FileObject.version == version)

    if uploader is not None:
        query = query.where(FileObject.uploader == uploader)

    # filter records that have ALL the URLs
    if urls:
        query = query.where(FileObject.urls.overlap(urls))
        # for u in urls:
        #     query = query.where(FileObject.url == u)

    # filter records that have ALL the ACL elements
    if acl:
        query = query.where(FileObject.acl.overlap(acl))
        # for ace in acl:
        #     query = query.where(FileObject.acl == ace)
    elif acl == []:
        query = query.where(FileObject.acl == None)

    # filter records that have ALL the authz elements
    if authz:
        query = query.where(FileObject.authz.overlap(authz))
        # for u in authz:
        #     sub = session.query(IndexRecordAuthz.did).filter(
        #         IndexRecordAuthz.resource == u
        #     )
        #     query = query.where(FileObject.did.in_(sub.subquery()))
    elif authz == []:
        query = query.where(FileObject.authz == None)

    # TODO
    # if hashes:
    #     for h, v in hashes.items():
    #         sub = session.query(IndexRecordHash.did)
    #         sub = sub.filter(
    #             and_(
    #                 IndexRecordHash.hash_type == h,
    #                 IndexRecordHash.hash_value == v,
    #             )
    #         )
    #         query = query.where(FileObject.did.in_(sub.subquery()))

    # TODO
    # if metadata:
    #     for k, v in metadata.items():
    #         sub = session.query(IndexRecordMetadata.did)
    #         sub = sub.filter(
    #             and_(
    #                 IndexRecordMetadata.key == k, IndexRecordMetadata.value == v
    #             )
    #         )
    #         query = query.where(FileObject.did.in_(sub.subquery()))

    # TODO
    # if urls_metadata:
    #     query = query.join(FileObject.urls).join(IndexRecordUrl.url_metadata)
    #     for url_key, url_dict in urls_metadata.items():
    #         query = query.where(IndexRecordUrlMetadata.url.contains(url_key))
    #         for k, v in url_dict.items():
    #             query = query.where(
    #                 IndexRecordUrl.url_metadata.any(
    #                     and_(
    #                         IndexRecordUrlMetadata.key == k,
    #                         IndexRecordUrlMetadata.value == v,
    #                     )
    #                 )
    #             )

    # TODO
    # if negate_params:
    #     query = self._negate_filter(session, query, **negate_params)

    # TODO
    # # joining url metadata will have duplicate results
    # # url or acl doesn't have duplicate results for current filter
    # # so we don't need to select distinct for these cases
    # if urls_metadata or negate_params:
    #     query = query.distinct(FileObject.did)

    if page is not None:
        # order by updated date so newly added stuff is
        # at the end (reduce risk that a new records ends up in a page
        # earlier on) and allows for some logic to check for newly added records
        # (e.g. parallelly processing from beginning -> middle and ending -> middle
        #       and as a final step, checking the "ending"+1 to see if there are
        #       new records).
        query = query.order_by(FileObject.updated_date)
    else:
        query = query.order_by(FileObject.did)

    if ids:
        query = query.where(FileObject.did.in_(ids))
    else:
        # only apply limit when ids is not provided
        query = query.limit(limit)

    if page is not None:
        query = query.offset(limit * page)

    def to_dict(item):
        r = item.to_dict()
        # starlette JSONResponse does not handle datetimes
        r["created_date"] = r["created_date"].strftime("%Y-%m-%dT%H:%M:%S")
        r["updated_date"] = r["updated_date"].strftime("%Y-%m-%dT%H:%M:%S")
        return r

    return [to_dict(i) for i in await query.gino.all()]


@mod.post("/index")
@mod.post("/index/")
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

    did, rev, baseid = await add_file_object(
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

    return JSONResponse(ret, HTTP_200_OK)


async def add_file_object(
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
