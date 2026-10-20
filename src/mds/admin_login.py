from authutils.token.fastapi import access_token
from fastapi import Depends, HTTPException, Security
from fastapi.security import (
    HTTPAuthorizationCredentials,
    HTTPBasic,
    HTTPBasicCredentials,
    HTTPBearer,
)
from gen3authz.client.arborist.async_client import ArboristClient
from starlette.status import HTTP_401_UNAUTHORIZED, HTTP_403_FORBIDDEN

from . import config, logger

security = HTTPBasic(auto_error=False)
bearer = HTTPBearer(auto_error=False)
arborist = ArboristClient()


async def admin_required(
    credentials: HTTPBasicCredentials = Depends(security),
    token: HTTPAuthorizationCredentials = Security(bearer),
):
    if config.DEBUG:
        logger.warning("Skipping authorization check")
        return

    if credentials:
        logger.info("Received Basic Auth credentials")
        for username, password in config.ADMIN_LOGINS:
            if credentials.username == username and credentials.password == password:
                return  # valid admin credentials
        logger.warning(
            "Invalid Basic Auth credentials. Attempting fallback to JWT token..."
        )

    service = "mds_gateway"
    method = "access"
    resource = "/mds_gateway"
    if token:
        # validate the token before handing it off to Arborist
        try:
            await access_token(
                "user",
                "openid",
                # TODO: remove "openid" - it's a workaround to accept old-fence tokens
                # that do not have the updated aud
                audience=["openid", "gen3"],
                allowed_issuers=list(config.ALLOWED_ISSUERS) or None,
                purpose="access",
            )(token)
        except Exception as e:
            logger.error(e.detail if hasattr(e, "detail") else e, exc_info=True)
            raise HTTPException(
                HTTP_401_UNAUTHORIZED,
                "Could not verify, parse, and/or validate scope from provided access token.",
            )
    if not token or not await arborist.auth_request(
        token.credentials, service, method, resource
    ):
        logger.error(
            f"Authorization error: token must have '{method}' access on {resource} for service '{service}'."
        )
        raise HTTPException(status_code=HTTP_403_FORBIDDEN)
