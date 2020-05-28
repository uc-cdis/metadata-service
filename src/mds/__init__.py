import logging
from . import config

logger = logging.getLogger("mds")

if config.DEBUG:
    logger.setLevel(logging.DEBUG)
