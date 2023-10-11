import functools
import logging
from typing import Mapping, Optional

# Logging config to be used in tests
LOGGING_CONFIG = {
    "version": 1,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        },
    },
    "handlers": {
        "default": {
            "level": "DEBUG",
            "class": "logging.StreamHandler",
            "formatter": "standard",
        },
    },
    "loggers": {
        "confluent_kafka": {
            "handlers": ["default"],
            "level": "DEBUG",
            "propagate": False,
        },
        "streamingdataframes": {
            "handlers": ["default"],
            "level": "DEBUG",
            "propagate": False,
        },
    },
}


def patch_logger_class():
    """
    Patch default logger class to output all data from "extra" in test logs
    """
    logger_cls = logging.getLoggerClass()
    # Patch makeRecord() of the default Logger class to print all "extra" fields in logs
    logger_cls.makeRecord = _patch_makeRecord(logger_cls.makeRecord)


def _patch_makeRecord(func_):
    @functools.wraps(func_)
    def makeRecord_wrapper(
        self,
        name: str,
        level: int,
        fn: str,
        lno: int,
        msg: object,
        args,
        exc_info,
        func: Optional[str] = None,
        extra: Optional[Mapping[str, object]] = None,
        sinfo: Optional[str] = None,
    ) -> logging.LogRecord:
        """
        Print all field from "extra" dict in the logs in tests
        """
        record = func_(
            self, name, level, fn, lno, msg, args, exc_info, func, extra, sinfo
        )
        if extra is not None:
            extra_str = " ".join(f"{k}={v}" for k, v in extra.items())
            record.msg += " " + extra_str
        return record

    return makeRecord_wrapper
