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
# an incompatible way. v2 of the TTL feature uses ``2``; absent/lower markers
# trigger ``IncompatibleStateStoreError`` at open time so operators reset
# the directory and let recovery rebuild from the changelog.
STATE_FORMAT_VERSION_KEY = b"__ttl_format_version__"
STATE_FORMAT_VERSION = 2
