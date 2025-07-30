from fastapi import Depends, HTTPException, Security
from fastapi.security import (
    HTTPAuthorizationCredentials,
    HTTPBasic,
    HTTPBasicCredentials,
    HTTPBearer,
)
from gen3authz.client.arborist.async_client import ArboristClient
from starlette.status import HTTP_403_FORBIDDEN

from . import config, logger

security = HTTPBasic(auto_error=False)
bearer = HTTPBearer(auto_error=False)
arborist = ArboristClient()


async def admin_required(
    credentials: HTTPBasicCredentials = Depends(security),
    token: HTTPAuthorizationCredentials = Security(bearer),
):
    if config.DEBUG:
        logger.debug("Skipping authorization check")

    if credentials:
        logger.info("Received Basic Auth credentials")
        for username, password in config.ADMIN_LOGINS:
            if credentials.username == username and credentials.password == password:
                return
        logger.warning(
            "Invalid Basic Auth credentials. Attempting fallback to JWT token..."
        )
    else:
        service = "mds_gateway"
        method = "access"
        resource = "/mds_gateway"
        if not token or not await arborist.auth_request(
            token.credentials, service, method, resource
        ):
            logger.error(
                f"Authorization error: token must have '{method}' access on {resource} for service '{service}'."
            )
            raise HTTPException(status_code=HTTP_403_FORBIDDEN)
