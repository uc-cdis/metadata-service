from mds.config import LOGGING_CONFIG

error_and_access_log_config = dict(
    #  XXX add gunicorn handler
    loggers={
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

gunicorn_logging_config = LOGGING_CONFIG.copy()
gunicorn_logging_config.update(error_and_access_log_config)
logconfig_dict = gunicorn_logging_config
#  logconfig_dict = error_and_access_log_config
print("Finished gunicorn.conf.py!")
