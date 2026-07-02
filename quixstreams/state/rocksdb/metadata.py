PROCESSED_OFFSET_KEY = b"__topic_offset__"
CHANGELOG_OFFSET_KEY = b"__changelog_offset__"

GLOBAL_COUNTER_CF_NAME = "__global-counter__"
GLOBAL_COUNTER_KEY = b"__global_counter__"

# TTL feature constants (see dev-planning/state-ttl/architecture.md).
# TTL_INDEX_CF_NAME is shared with quixstreams.state.metadata so the base
# transaction can route writes for it locally, off the changelog.
from quixstreams.state.metadata import TTL_INDEX_CF_NAME  # noqa: E402, F401

# Highest record event-time observed by any transaction on this partition,
# persisted to the metadata CF on every flush so the sweep / read-time filter
# survive restarts.
TTL_HIGH_WATER_KEY = b"__ttl_high_water_ms__"

# On-disk format-version marker. Bumped whenever the value layout changes in
# an incompatible way. v3 of the TTL feature uses ``2``; the marker is written
# only when a partition flips into TTL mode (see ``TTL_ENABLED_KEY``). Stores
# that never see a ``state.set(..., ttl=...)`` write stay marker-free and are
# byte-identical to the v3.23.6 on-disk layout.
STATE_FORMAT_VERSION_KEY = b"__ttl_format_version__"
STATE_FORMAT_VERSION = 2

# Per-partition opt-in flag for the TTL machinery. Absent (or empty) means the
# partition is in legacy mode: writes are not stamped, ``__ttl_index__`` does
# not exist, the sweep is a no-op, and recovery replays values verbatim.
# Present-and-truthy means the partition has been flipped into TTL mode by the
# framework on the first ``state.set(..., ttl=...)`` write that landed on a
# fresh (empty) default CF; once flipped, it stays flipped.
TTL_ENABLED_KEY = b"__ttl_enabled__"
