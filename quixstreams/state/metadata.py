import enum

PREFIX_SEPARATOR = b"|"

CHANGELOG_CF_MESSAGE_HEADER = "__column_family__"
CHANGELOG_PROCESSED_OFFSET_MESSAGE_HEADER = "__processed_tp_offset__"
METADATA_CF_NAME = "__metadata__"

DEFAULT_PREFIX = b""


class Marker(enum.Enum):
    UNDEFINED = 1
    DELETED = 2
