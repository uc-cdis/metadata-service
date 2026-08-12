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
    """Enforces authorization, checking for a specific "mds_gateway access permission".

    This function can be used as an extra check to specific endpoints.

    If basic username + password is provided: if will check if the username + password
    is part of the list of configured ADMIN_LOGINS.
    If no username + password is provided OR it is not an ADMIN login, if falls back
    to token (below).

    If a token is provided: it verifies that the provided bearer token has
    `access` permission on the
    `/mds_gateway` resource for the
    `mds_gateway` service by querying Arborist.

    In debug mode, the authorization check (i.e. Arborist query) is skipped.

    Args below are expected to be filled automatically by API framework
    when this method is added as a dependency to the API router.

    Args:
        credentials: (optional) HTTP Basic authentication credentials provided by the client.
        token: (optional) Bearer token credentials extracted from the Authorization header.

    Returns:
        None

    Raises:
        HTTPException: With status code 403 if the token is missing or does not
        have the required authorization.
    """
    if config.DEBUG:
        logger.warning("Skipping authorization check")
        return

    if credentials:
        logger.info("Received Basic Auth credentials")
        for username, password in config.ADMIN_LOGINS:
            if credentials.username == username and credentials.password == password:
                # valid admin credentials
                return
        logger.warning(
            "Invalid Basic Auth credentials. Attempting fallback to JWT token..."
        )

    method = "access"
    resource = "/mds_gateway"
    service = "mds_gateway"
    if not token or not await arborist.auth_request(
        token.credentials, service, method, resource
    ):
        logger.error(
            f"Authorization error: token must have '{method}' access on {resource} for service '{service}'."
        )
        raise HTTPException(status_code=HTTP_403_FORBIDDEN)


async def metadata_queries_access_required(
    token: HTTPAuthorizationCredentials = Security(bearer),
):
    """Enforces authorization, checking for a specific "metadata query permission".

    This function can be used as an extra check to specific endpoints.
    It verifies that the provided bearer token has
    `access` permission on the
    `/mds_metadata_queries` resource for the
    `mds_metadata_queries` service by querying Arborist.

    In debug mode, the authorization check (i.e. Arborist query) is skipped.

    Args below are expected to be filled automatically by API framework
    when this method is added as a dependency to the API router.

    Args:
        token: Bearer token credentials extracted from the Authorization header.

    Returns:
        None

    Raises:
        HTTPException: With status code 403 if the token is missing or does not
        have the required authorization.
    """
    if config.DEBUG:
        logger.warning("Skipping authorization check")
        return

    method = "access"
    resource = "/mds_metadata_queries"
    service = "mds_metadata_queries"
    if not token or not await arborist.auth_request(
        token.credentials, service, method, resource
    ):
        logger.error(
            f"Authorization error: token must have '{method}' access on {resource} for service '{service}'."
        )
        raise HTTPException(status_code=HTTP_403_FORBIDDEN)
