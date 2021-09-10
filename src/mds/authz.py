from authutils.token.fastapi import access_token
from fastapi import Depends, HTTPException, Security
from fastapi.security import (
    HTTPAuthorizationCredentials,
    HTTPBasic,
    HTTPBasicCredentials,
    HTTPBearer,
)
from starlette.requests import Request
from starlette.status import HTTP_401_UNAUTHORIZED, HTTP_403_FORBIDDEN

from gen3authz.client.arborist.async_client import ArboristClient
from gen3authz.client.arborist.errors import ArboristError

from . import config, logger


# auto_error=False prevents FastAPI from raising a 403 when the request
# is missing an Authorization header. Instead, we want to return a 401
# to signify that we did not recieve valid credentials
bearer = HTTPBearer(auto_error=False)
security = HTTPBasic(auto_error=False)
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


class Auth:
    def __init__(
        self,
        request: Request,
        bearer_token: HTTPAuthorizationCredentials = Security(bearer),
    ):
        self.bearer_token = bearer_token

    async def get_token_claims(self) -> dict:
        if not self.bearer_token:
            err_msg = "Must provide an access token."
            logger.error(err_msg)
            raise HTTPException(
                HTTP_401_UNAUTHORIZED,
                err_msg,
            )

        try:
            # NOTE: token can be None if no Authorization header was provided, we
            # expect this to cause a downstream exception since it is invalid
            token_claims = await access_token("user", "openid", purpose="access")(
                self.bearer_token
            )
        except Exception as e:
            logger.error(f"Could not get token claims:\n{e}", exc_info=True)
            raise HTTPException(
                HTTP_401_UNAUTHORIZED,
                "Could not verify, parse, and/or validate scope from provided access token.",
            )

        return token_claims

    async def authorize(
        self,
        method: str,
        resources: list,
        throw: bool = True,
    ) -> bool:
        token = (
            self.bearer_token.credentials
            if self.bearer_token and hasattr(self.bearer_token, "credentials")
            else None
        )

        try:
            # TODO POC code - the service should not be "indexd"
            authorized = await arborist.auth_request(token, "indexd", method, resources)
        except ArboristError as e:
            logger.error(f"Error while talking to arborist: {e}")
            authorized = False

        # TODO add indexd admin logic for indexd endpoints. create mds admin
        # logic instead? or use existing mds_gateway?
        if not authorized:
            logger.error(
                f"Authorization error: user must have '{method}' access on {resources} for service 'audit'."
            )
            if throw:
                raise HTTPException(
                    HTTP_403_FORBIDDEN,
                    "Permission denied",
                )

        return authorized
