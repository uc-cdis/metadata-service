"""
Usage:
- Run app: python run.py
- Generate openapi docs: python run.py openapi
"""

import os
import sys
import uvicorn
import yaml
import logging.config

from mds.main import get_app
import mds.config

#  from mds.config import LOGGING_CONFIG as MDS_LOGGING_CONFIG

#  logging.config.dictConfig(mds.config.LOGGING_CONFIG)
error_and_access_log_config = dict(
    version=1,
    disable_existing_loggers=False,
    loggers={
        "uvicorn": {
            "level": "INFO",
            "handlers": ["console", "error_console"],
            "propagate": False,
            "qualname": "uvicorn",
        },
        "uvicorn.error": {
            "level": "INFO",
            "handlers": ["console", "error_console"],
            "propagate": False,
            "qualname": "uvicorn.error",
        },
        "uvicorn.access": {
            "level": "INFO",
            "handlers": ["console", "error_console"],
            "propagate": False,
            "qualname": "uvicorn.access",
        },
    },
)
#  XXX deepcopy
uvicorn_logging_config = mds.config.LOGGING_CONFIG.copy()
uvicorn_logging_config["loggers"].update(error_and_access_log_config["loggers"])
#  logconfig_dict = gunicorn_logging_config

#  logging_config.update(mds.config.LOGGING_CONFIG)
logging.config.dictConfig(uvicorn_logging_config)
#  logging.config.dictConfig(error_and_access_log_config)


print("configured logging from run.py!")


CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


if __name__ == "__main__":
    if sys.argv[-1] == "openapi":
        schema = get_app().openapi()
        path = os.path.join(CURRENT_DIR, "docs/openapi.yaml")
        yaml.Dumper.ignore_aliases = lambda *args: True
        with open(path, "w+") as f:
            yaml.dump(schema, f, default_flow_style=False)
        print(f"Saved docs at {path}")
    else:
        uvicorn.run("mds.asgi:app", reload=True, log_config=uvicorn_logging_config)
