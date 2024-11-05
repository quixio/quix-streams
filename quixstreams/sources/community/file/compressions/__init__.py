# ruff: noqa: F403
# ruff: noqa: F405
from .base import *
from .gzip import *

COMPRESSION_MAPPER = {"gz": GZipDecompressor, "gzip": GZipDecompressor}
