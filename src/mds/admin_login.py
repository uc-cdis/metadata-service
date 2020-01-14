from fastapi import Depends, HTTPException
from fastapi.security import HTTPBasicCredentials, HTTPBasic
from starlette.status import HTTP_403_FORBIDDEN

from . import config

security = HTTPBasic(auto_error=False)


def admin_required(credentials: HTTPBasicCredentials = Depends(security)):
    if not config.DEBUG:
        for username, password in config.ADMIN_LOGINS:
            if (
                credentials
                and credentials.username == username
                and credentials.password == password
            ):
                break
        else:
            raise HTTPException(status_code=HTTP_403_FORBIDDEN)
