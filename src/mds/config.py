from starlette.config import Config
from starlette.datastructures import CommaSeparatedStrings
from starlette.datastructures import Secret


class CommaSeparatedLogins(CommaSeparatedStrings):
    def __init__(self, value):
        super().__init__(value)
        self._items = [item.split(":") for item in self._items]


config = Config(".env")

DEBUG = config("DEBUG", cast=bool, default=True)
TESTING = config("TESTING", cast=bool, default=False)
URL_PREFIX = config("URL_PREFIX", default="/" if DEBUG else "/mds")

DB_HOST = config("DB_HOST", default=None)
DB_PORT = config("DB_PORT", cast=int, default=None)
DB_USER = config("DB_USER", default=None)
DB_PASSWORD = config("DB_PASSWORD", cast=Secret, default=None)
DB_DATABASE = config("DB_DATABASE", default=None)
DB_MIN_SIZE = config("DB_MIN_SIZE", cast=int, default=1)
DB_MAX_SIZE = config("DB_MAX_SIZE", cast=int, default=10)
DB_CONNECT_RETRIES = config("DB_CONNECT_RETRIES", cast=int, default=32)

ADMIN_LOGINS = config("ADMIN_LOGINS", cast=CommaSeparatedLogins, default=[])

if TESTING:
    DB_DATABASE = "test_" + (DB_DATABASE or "mds")
    TEST_KEEP_DB = config("TEST_KEEP_DB", cast=bool, default=False)
