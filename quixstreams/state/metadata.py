import enum

SEPARATOR = b"|"
SEPARATOR_LENGTH = len(SEPARATOR)

CHANGELOG_CF_MESSAGE_HEADER = "__column_family__"
METADATA_CF_NAME = "__metadata__"

DEFAULT_PREFIX = b""


class Marker(enum.Enum):
    UNDEFINED = 1
    DELETED = 2
