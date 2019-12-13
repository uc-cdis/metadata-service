from starlette.config import Config
from starlette.datastructures import Secret

config = Config(".env")

DEBUG = config("DEBUG", cast=bool, default=False)
TESTING = config("TESTING", cast=bool, default=False)

DB_HOST = config("DB_HOST", default=None)
DB_PORT = config("DB_PORT", cast=int, default=None)
DB_USER = config("DB_USER", default=None)
DB_PASSWORD = config("DB_PASSWORD", cast=Secret, default=None)
DB_DATABASE = config("DB_DATABASE", default=None)
DB_MIN_SIZE = config("DB_MIN_SIZE", cast=int, default=1)
DB_MAX_SIZE = config("DB_MAX_SIZE", cast=int, default=10)


if TESTING:
    DB_DATABASE = "test_" + (DB_DATABASE or "mds")
    TEST_KEEP_DB = config("TEST_KEEP_DB", cast=bool, default=False)
