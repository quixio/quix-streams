# For Python 3.8, we use testcontainers==3.7.1,
# which lacks the high-level network API.
# This can be removed once we stop supporting Python 3.8.
try:
    from testcontainers.core.network import Network
except ImportError:
    from .network import Network

__all__ = ["Network"]
