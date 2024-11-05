# ruff: noqa: F403
# ruff: noqa: F405
from .base import *
from .json import *
from .parquet import *

FORMATS = {
    "json": JSONFormat,
    "parquet": ParquetFormat,
}
