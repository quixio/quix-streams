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

_STATE_LOG_LEVEL_ENV = "QUIXSTREAMS_STATE_LOG_LEVEL"
_STATE_LOGGER_NAME = f"{LOGGER_NAME}.state"
# NOTSET is deliberately excluded: it maps to level 0, which would make the
# dedicated state handler pass everything the state logger emits with no floor.
_ACCEPTED_STATE_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")

# Dedicated handler for the ``quixstreams.state`` namespace, created lazily the
# first time the override is applied (see ``_apply_state_log_level_override``).
# Module-scoped so a re-run (e.g. subprocess reconfigure) reuses the same handler
# instead of stacking duplicates.
_STATE_HANDLER: Optional[logging.Handler] = None


def _apply_state_log_level_override() -> None:
    """Scoped verbosity for the ``quixstreams.state`` namespace, driven by the
    ``QUIXSTREAMS_STATE_LOG_LEVEL`` env var.

    Lets an operator raise ``quixstreams.state.*`` to DEBUG without flipping the
    whole app to DEBUG. Strictly scoped to the ``quixstreams.state`` logger: it
    attaches a *dedicated* handler to that logger at the requested level and stops
    propagation to the parent so the shared root handler is never mutated and
    non-state loggers are entirely unaffected (the earlier build lowered the
    shared ``_DEFAULT_HANDLER`` level — a global side effect this avoids).

    Unset/empty is a no-op: the state namespace inherits the app loglevel and
    propagates to the shared handler as usual. An unrecognized value warns once
    and is ignored (the app still starts).
    """
    global _STATE_HANDLER

    raw = os.environ.get(_STATE_LOG_LEVEL_ENV)
    if not raw:
        return  # unset/empty -> no-op, inherit app level + shared handler

    level_name = raw.strip().upper()
    if level_name not in _ACCEPTED_STATE_LOG_LEVELS:
        logger.warning(
            "Ignoring invalid %s=%r; expected one of %s.",
            _STATE_LOG_LEVEL_ENV,
            raw,
            ", ".join(_ACCEPTED_STATE_LOG_LEVELS),
        )
        return

    level = logging.getLevelName(level_name)  # accepted name -> int
    state_logger = logging.getLogger(_STATE_LOGGER_NAME)
    state_logger.setLevel(level)

    # Give the state namespace its OWN handler at the requested level so its
    # (possibly more verbose) records reach stderr WITHOUT touching the shared
    # root handler. Route state records solely through this handler
    # (propagate=False) so the parent handler neither re-filters nor
    # double-emits them. Created once; subsequent calls only refresh the level.
    if _STATE_HANDLER is None:
        _STATE_HANDLER = logging.StreamHandler(stream=sys.stderr)
        if _DEFAULT_HANDLER.formatter is not None:
            _STATE_HANDLER.setFormatter(_DEFAULT_HANDLER.formatter)
        state_logger.addHandler(_STATE_HANDLER)
        state_logger.propagate = False
    _STATE_HANDLER.setLevel(level)


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
            _apply_state_log_level_override()
            return True

    # Configuring logger
    logger.setLevel(loglevel)
    logger.propagate = False
    _DEFAULT_HANDLER.setFormatter(logging.Formatter(formatter))
    _DEFAULT_HANDLER.setLevel(loglevel)
    logger.addHandler(_DEFAULT_HANDLER)
    _apply_state_log_level_override()
    return True
