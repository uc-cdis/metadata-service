import copy
import logging.config

from . import config


_logging_config = copy.deepcopy(config.DEFAULT_LOGGING_CONFIG)
_logging_config.update(config.MDS_LOGGER_CONFIG)
logging.config.dictConfig(_logging_config)

logger = logging.getLogger("mds")
