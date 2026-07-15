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
    whole app to DEBUG. It lowers the ``quixstreams.state`` logger level and
    attaches a *dedicated* handler to that logger at the requested level so the
    (possibly more verbose) state records reach stderr WITHOUT touching the shared
    root ``_DEFAULT_HANDLER`` (the earlier build lowered that shared handler's
    level — a global side effect this avoids).

    Propagation is deliberately LEFT ENABLED (finding 3, review batch 4): a custom
    handler an operator has attached to the ``quixstreams`` (or root) logger must
    keep receiving ``quixstreams.state.*`` records — including ERROR/CRITICAL —
    which is exactly when someone enables this env var to debug a migration. An
    earlier build set ``propagate = False`` here, which silently stole those
    records from custom handlers. The trade-off is minor duplicate emission when
    an upstream handler is also present (the dedicated handler AND the upstream
    handler each emit), which is strictly preferable to dropping errors.

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
    # Keep propagation ON so any custom/upstream handler on ``quixstreams`` or the
    # root logger still receives state records (finding 3). Set explicitly (rather
    # than relying on the default) so a re-run that follows an older
    # ``propagate = False`` build re-enables it idempotently.
    state_logger.propagate = True

    # Give the state namespace its OWN handler at the requested level so its
    # (possibly more verbose) records still reach stderr even when the app
    # configured no handler at all (or only a higher-level one that would filter
    # DEBUG state records out). Created once; subsequent calls only refresh the
    # level. Because propagation stays on, an upstream handler may ALSO emit these
    # records — accepted minor duplication over dropping errors (see docstring).
    if _STATE_HANDLER is None:
        _STATE_HANDLER = logging.StreamHandler(stream=sys.stderr)
        if _DEFAULT_HANDLER.formatter is not None:
            _STATE_HANDLER.setFormatter(_DEFAULT_HANDLER.formatter)
        state_logger.addHandler(_STATE_HANDLER)
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
    try:
        return _configure_root_logger(loglevel, name, pid)
    finally:
        # #15 (review batch 3): the QUIXSTREAMS_STATE_LOG_LEVEL override is
        # independent of the main-logger config (it attaches a dedicated handler to
        # ``quixstreams.state``), so it must run on EVERY return path — including
        # ``loglevel is None`` and the "app already owns the quixstreams handlers"
        # early returns — not only when we (re)configure the default handler. The
        # ``finally`` guarantees exactly that regardless of which branch is taken;
        # the override is self-contained and idempotent (the dedicated handler is
        # created once). An unrecognized value warns once inside the override.
        _apply_state_log_level_override()


def _configure_root_logger(
    loglevel: Optional[Union[int, LogLevel]],
    name: str,
    pid: bool,
) -> bool:
    """Configure the ``quixstreams`` root logger/handler. Returns True iff the
    config was (re)applied. Factored out of :func:`configure_logging` so the
    ``quixstreams.state`` override can run on every return path (#15)."""
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
