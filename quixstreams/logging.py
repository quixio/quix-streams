import logging
import os
import sys
from typing import Literal, Optional, Union

__all__ = ("configure_logging", "LogLevel", "LOGGER_NAME")

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
    loglevel: Optional[Union[int, LogLevel]],
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

    if pid:
        formatter = (
            f"[%(asctime)s] [%(levelname)s] [{name}] [{os.getpid()}] : %(message)s"
        )
    else:
        formatter = f"[%(asctime)s] [%(levelname)s] [{name}] : %(message)s"

    if logger.handlers:
        if len(logger.handlers) != 1 or logger.handlers[0] is not _DEFAULT_HANDLER:
            # There's a pre-configured handler for "quixstreams", leave it as it is
            return False
        else:
            # The pre-configured handler for "quixstreams" is the default handler.
            # Reconfigure the formatter in case we are in a subprocess and the logger
            # was configured by mistake by the Application.
            _DEFAULT_HANDLER.setFormatter(logging.Formatter(formatter))
            return True

    # Configuring logger
    logger.setLevel(loglevel)
    logger.propagate = False
    _DEFAULT_HANDLER.setFormatter(logging.Formatter(formatter))
    _DEFAULT_HANDLER.setLevel(loglevel)
    logger.addHandler(_DEFAULT_HANDLER)
    return True
