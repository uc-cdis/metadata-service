import cdislogging
from . import config


logger = cdislogging.get_logger(__name__, log_level="debug" if config.DEBUG else "info")
