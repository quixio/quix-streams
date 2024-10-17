from typing import NewType

PREFIX_SEPARATOR = b"|"

CHANGELOG_CF_MESSAGE_HEADER = "__column_family__"
CHANGELOG_PROCESSED_OFFSET_MESSAGE_HEADER = "__processed_tp_offset__"
METADATA_CF_NAME = "__metadata__"

Undefined = NewType("Undefined", object)
UNDEFINED = Undefined(object())
DELETED = Undefined(object())

DEFAULT_PREFIX = b""
