from fastapi import Depends, HTTPException, Security
from fastapi.security import (
    HTTPAuthorizationCredentials,
    HTTPBasic,
    HTTPBasicCredentials,
    HTTPBearer,
)
from gen3authz.client.arborist.async_client import ArboristClient
from starlette.status import HTTP_403_FORBIDDEN

from . import config

security = HTTPBasic(auto_error=False)
bearer = HTTPBearer(auto_error=False)
arborist = ArboristClient()


async def admin_required(
    credentials: HTTPBasicCredentials = Depends(security),
    token: HTTPAuthorizationCredentials = Security(bearer),
):
    if not config.DEBUG:
        for username, password in config.ADMIN_LOGINS:
            if (
                credentials
                and credentials.username == username
                and credentials.password == password
            ):
                break
        else:
            if not token or not await arborist.auth_request(
                token.credentials, "mds_gateway", "access", "/mds_gateway"
            ):
                raise HTTPException(status_code=HTTP_403_FORBIDDEN)
