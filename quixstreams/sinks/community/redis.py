import json
import logging
import time
from typing import Any, Callable, Optional, Union

try:
    import redis
except ImportError as exc:
    raise ImportError(
        f"Package {exc.name} is missing: "
        'run "pip install quixstreams[redis]" to use RedisSink'
    ) from exc

from quixstreams.sinks import (
    BatchingSink,
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
    SinkBatch,
)

__all__ = ("RedisSink",)

logger = logging.getLogger(__name__)


class RedisSink(BatchingSink):
    def __init__(
        self,
        host: str,
        port: int,
        db: int,
        value_serializer: Callable[[Any], Union[bytes, str]] = json.dumps,
        key_serializer: Optional[Callable[[Any, Any], Union[bytes, str]]] = None,
        password: Optional[str] = None,
        socket_timeout: float = 30.0,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
        **kwargs,
    ) -> None:
        """
        A connector to sink processed data to Redis.
        It batches the processed records in memory per topic partition, and flushes them to Redis at the checkpoint.

        :param host: Redis host.
        :param port: Redis port.
        :param db: Redis DB number.
        :param value_serializer: a callable to serialize the value to string or bytes
            (defaults to json.dumps).
        :param key_serializer: an optional callable to serialize the key to string or bytes.
            If not provided, the Kafka message key will be used as is.
        :param password: Redis password, optional.
        :param socket_timeout: Redis socket timeout.
            Default - 30s.
        :param on_client_connect_success: An optional callback made after successful
            client authentication, primarily for additional logging.
        :param on_client_connect_failure: An optional callback made after failed
            client authentication (which should raise an Exception).
            Callback should accept the raised Exception as an argument.
            Callback must resolve (or propagate/re-raise) the Exception.
        :param kwargs: Additional keyword arguments passed to the `redis.Redis` instance.
        """
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )

        self._key_serializer = key_serializer
        self._value_serializer = value_serializer
        self._client: Optional[redis.Redis] = None
        self._client_settings = {
            "host": host,
            "port": port,
            "db": db,
            "password": password,
            "socket_timeout": socket_timeout,
            **kwargs,
        }
        self._redis_uri = (
            f"{self._client_settings['host']}:"
            f"{self._client_settings['port']}/"
            f"{self._client_settings['db']}"
        )

    def setup(self):
        self._client = redis.Redis(**self._client_settings)
        self._client.info()

    def write(self, batch: SinkBatch) -> None:
        # Execute Redis updates atomically using a transaction pipeline
        start = time.monotonic()
        with self._client.pipeline(transaction=True) as pipe:
            for item in batch:
                key = item.key
                if self._key_serializer is not None:
                    key = self._key_serializer(key, item.value)
                value = self._value_serializer(item.value)
                pipe.set(key, value)
            keys_updated = len(pipe)
            pipe.execute(raise_on_error=True)
        time_elapsed = round(time.monotonic() - start, 4)
        logger.debug(
            f'Updated {keys_updated} keys in a Redis database "{self._redis_uri}" '
            f"time_elapsed={time_elapsed}s"
        )
