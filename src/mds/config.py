from sqlalchemy.engine.url import make_url, URL
from starlette.config import Config
from starlette.datastructures import CommaSeparatedStrings
from starlette.datastructures import Secret


class CommaSeparatedLogins(CommaSeparatedStrings):
    def __init__(self, value):
        super().__init__(value)
        self._items = [item.split(":") for item in self._items]


config = Config(".env")

# Server

DEBUG = config("DEBUG", cast=bool, default=True)
TESTING = config("TESTING", cast=bool, default=False)
URL_PREFIX = config("URL_PREFIX", default="/" if DEBUG else "/mds")
USE_AGG_MDS = config("USE_AGG_MDS", cast=bool, default=False)
AGG_MDS_NAMESPACE = config("AGG_MDS_NAMESPACE", default="default_namespace")
ES_ENDPOINT = config("GEN3_ES_ENDPOINT", default="http://localhost:9200")

# Database

DB_DRIVER = config("DB_DRIVER", default="postgresql")
DB_HOST = config("DB_HOST", default=None)
DB_PORT = config("DB_PORT", cast=int, default=None)
DB_USER = config("DB_USER", default=None)
DB_PASSWORD = config("DB_PASSWORD", cast=Secret, default=None)
DB_DATABASE = config("DB_DATABASE", default=None)

if TESTING:
    DB_DATABASE = "test_" + (DB_DATABASE or "metadata")
    TEST_KEEP_DB = config("TEST_KEEP_DB", cast=bool, default=False)

DB_DSN = config(
    "DB_DSN",
    cast=make_url,
    default=URL(
        drivername=DB_DRIVER,
        username=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        database=DB_DATABASE,
    ),
)
DB_MIN_SIZE = config("DB_MIN_SIZE", cast=int, default=1)  # deprecated
DB_POOL_MIN_SIZE = config("DB_POOL_MIN_SIZE", cast=int, default=DB_MIN_SIZE)
DB_MAX_SIZE = config("DB_MAX_SIZE", cast=int, default=10)  # deprecated
DB_POOL_MAX_SIZE = config("DB_POOL_MAX_SIZE", cast=int, default=DB_MAX_SIZE)
DB_ECHO = config("DB_ECHO", cast=bool, default=False)
DB_SSL = config("DB_SSL", default=None)
DB_USE_CONNECTION_FOR_REQUEST = config(
    "DB_USE_CONNECTION_FOR_REQUEST", cast=bool, default=True
)
DB_CONNECT_RETRIES = config("DB_CONNECT_RETRIES", cast=int, default=32)  # deprecated
DB_RETRY_LIMIT = config("DB_RETRY_LIMIT", cast=int, default=DB_CONNECT_RETRIES)
DB_RETRY_INTERVAL = config("DB_RETRY_INTERVAL", cast=int, default=1)


# Security

ADMIN_LOGINS = config("ADMIN_LOGINS", cast=CommaSeparatedLogins, default=[])

# option to force authutils to prioritize ALLOWED_ISSUERS setting over the issuer from
# token when redirecting, used during local docker compose setup when the
# services are on different containers but the hostname is still localhost
# When FORCE_ISSUER is set to True the ALLOWED_ISSUER must be set and contain the 
# localhost URL http://fence-service/,https://localhost/user, while USER_API should contain
# the new issuer http://fence-service/
FORCE_ISSUER = config("FORCE_ISSUER", default=None)
USER_API = config("USER_API", cast=str, default="")
ALLOWED_ISSUERS = set(config("ALLOWED_ISSUERS", cast=CommaSeparatedStrings, default=""))


# Other Services

INDEXING_SERVICE_ENDPOINT = config(
    "INDEXING_SERVICE_ENDPOINT", cast=str, default="http://indexd-service"
)
DATA_ACCESS_SERVICE_ENDPOINT = config(
    "DATA_ACCESS_SERVICE_ENDPOINT", cast=str, default="http://fence-service"
)
