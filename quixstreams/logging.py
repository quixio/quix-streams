import logging
import sys
from typing import Literal, Optional

__all__ = ("configure_logging", "LogLevel")

LogLevel = Literal[
    "CRITICAL",
    "ERROR",
    "WARNING",
    "INFO",
    "DEBUG",
    "NOTSET",
]

_LOGGER_NAME = "quixstreams"

_DEFAULT_FORMATTER = logging.Formatter(
    f"[%(asctime)s] [%(levelname)s] [{_LOGGER_NAME}] : %(message)s"
)
_DEFAULT_HANDLER = logging.StreamHandler(stream=sys.stderr)

logger = logging.getLogger(_LOGGER_NAME)


def configure_logging(
    loglevel: Optional[LogLevel],
) -> bool:
    """
    Configure "quixstreams" logger.

    >***NOTE:*** If "quixstreams" logger already has pre-defined handlers
    (e.g. logging has already been configured via `logging`, or the function
    is called twice), it will skip configuration and return `False`.

    :param loglevel: a valid log level as a string or None.
        If None passed, this function is no-op and no logging will be configured.
    :return: True if logging config has been updated, otherwise False.
    """
    if loglevel is None:
        # Skipping logging configuration
        return False

    if logger.handlers:
        # There's a pre-configured handler for "quixstreams", leave it as it is
        return False

    # Configuring logger
    logger.setLevel(loglevel)
    logger.propagate = False
    _DEFAULT_HANDLER.setFormatter(_DEFAULT_FORMATTER)
    _DEFAULT_HANDLER.setLevel(loglevel)
    logger.addHandler(_DEFAULT_HANDLER)
    return True
