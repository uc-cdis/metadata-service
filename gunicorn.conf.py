import copy
from mds.config import DEFAULT_LOGGING_CONFIG

GUNICORN_LOGGERS_CONFIG = dict(
    loggers={
        "gunicorn": {
            "level": "INFO",
            "handlers": ["console", "error_console"],
            "propagate": False,
            "qualname": "gunicorn",
        },
        "gunicorn.error": {
            "level": "INFO",
            "handlers": ["console", "error_console"],
            "propagate": False,
            "qualname": "gunicorn.error",
        },
        "gunicorn.access": {
            "level": "INFO",
            "handlers": ["console", "error_console"],
            "propagate": False,
            "qualname": "gunicorn.access",
        },
    },
)

logging_config = copy.deepcopy(DEFAULT_LOGGING_CONFIG)
logging_config.update(GUNICORN_LOGGERS_CONFIG)
logconfig_dict = logging_config
