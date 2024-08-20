import os
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

LOGGER_NAME = "quixstreams"

_DEFAULT_HANDLER = logging.StreamHandler(stream=sys.stderr)

logger = logging.getLogger(LOGGER_NAME)


def configure_logging(
    loglevel: Optional[LogLevel],
    name: str = LOGGER_NAME,
    pid: bool = False,
) -> bool:
    """
    Configure "quixstreams" logger.

    >***NOTE:*** If "quixstreams" logger already has pre-defined handlers
    (e.g. logging has already been configured via `logging`, or the function
    is called twice), it will skip configuration and return `False`.

    :param loglevel: a valid log level as a string or None.
        If None passed, this function is no-op and no logging will be configured.
    :param name: the log name included in the output
    :param pid: if True include the process PID in the logs
    :return: True if logging config has been updated, otherwise False.
    """
    if loglevel is None:
        # Skipping logging configuration
        return False

    if logger.handlers:
        # There's a pre-configured handler for "quixstreams", leave it as it is
        return False

    if pid:
        formatter = (
            f"[%(asctime)s] [%(levelname)s] [{name}] [{os.getpid()}] : %(message)s"
        )
    else:
        formatter = f"[%(asctime)s] [%(levelname)s] [{name}] : %(message)s"

    # Configuring logger
    logger.setLevel(loglevel)
    logger.propagate = False
    _DEFAULT_HANDLER.setFormatter(logging.Formatter(formatter))
    _DEFAULT_HANDLER.setLevel(loglevel)
    logger.addHandler(_DEFAULT_HANDLER)
    return True
