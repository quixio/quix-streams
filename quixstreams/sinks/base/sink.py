import abc
import logging
from typing import Any, Callable, Optional

from quixstreams.models import HeadersTuples
from quixstreams.sinks.base.batch import SinkBatch

logger = logging.getLogger(__name__)


ClientConnectSuccessCallback = Callable[[], None]
ClientConnectFailureCallback = Callable[[Optional[Exception]], None]


def _default_on_client_connect_success():
    logger.info("CONNECTED!")


def _default_on_client_connect_failure(exception: Exception):
    logger.error(f"ERROR! - Failed while connecting to client: {exception}")
    raise exception


class BaseSink(abc.ABC):
    """
    This is a base class for all sinks.

    Subclass it and implement its methods to create your own sink.

    Note that Sinks are currently in beta, and their design may change over time.
    """

    def __init__(
        self,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ):
        """
        :param on_client_connect_success: An optional callback made after successful
            client authentication, primarily for additional logging.
        :param on_client_connect_failure: An optional callback made after failed
            client authentication (which should raise an Exception).
            Callback should accept the raised Exception as an argument.
            Callback must resolve (or propagate/re-raise) the Exception.
        """
        self._on_client_connect_success = (
            on_client_connect_success or _default_on_client_connect_success
        )
        self._on_client_connect_failure = (
            on_client_connect_failure or _default_on_client_connect_failure
        )

    @abc.abstractmethod
    def flush(self):
        """
        This method is triggered by the Checkpoint class when it commits.

        You can use `flush()` to write the batched data to the destination (in case of
        a batching sink), or confirm the delivery of the previously sent messages
        (in case of a streaming sink).

        If flush() fails, the checkpoint will be aborted.
        """

    @abc.abstractmethod
    def add(
        self,
        value: Any,
        key: Any,
        timestamp: int,
        headers: HeadersTuples,
        topic: str,
        partition: int,
        offset: int,
    ):
        """
        This method is triggered on every new processed record being sent to this sink.

        You can use it to accumulate batches of data before sending them outside, or
        to send results right away in a streaming manner and confirm a delivery later
        on flush().
        """

    def setup(self):
        """
        When applicable, set up the client here along with any validation to affirm a
        valid/successful authentication/connection.
        """

    def start(self):
        """
        Called as part of `Application.run()` to initialize the sink's client.
        Allows using a callback pattern around the connection attempt.
        """
        try:
            self.setup()
            self._on_client_connect_success()
        except Exception as e:
            self._on_client_connect_failure(e)

    def on_paused(self):
        """
        This method is triggered when the sink is paused due to backpressure, when
        the `SinkBackpressureError` is raised.

        Here you can react to the backpressure events.
        """


class BatchingSink(BaseSink):
    """
    A base class for batching sinks, that need to accumulate the data first before
    sending it to the external destinations.

    Examples: databases, objects stores, and other destinations where
    writing every message is not optimal.

    It automatically handles batching, keeping batches in memory per topic-partition.

    You may subclass it and override the `write()` method to implement a custom
    batching sink.
    """

    _batches: dict[tuple[str, int], SinkBatch]

    def __init__(
        self,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ):
        """
        :param on_client_connect_success: An optional callback made after successful
            client authentication, primarily for additional logging.
        :param on_client_connect_failure: An optional callback made after failed
            client authentication (which should raise an Exception).
            Callback should accept the raised Exception as an argument.
            Callback must resolve (or propagate/re-raise) the Exception.
        """
        super().__init__(
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )
        self._batches = {}

    def __repr__(self):
        return f"<BatchingSink: {self.__class__.__name__}>"

    @abc.abstractmethod
    def write(self, batch: SinkBatch):
        """
        This method implements actual writing to the external destination.

        It may also raise `SinkBackpressureError` if the destination cannot accept new
        writes at the moment.
        When this happens, the accumulated batch is dropped and the app pauses the
        corresponding topic partition.
        """

    def add(
        self,
        value: Any,
        key: Any,
        timestamp: int,
        headers: HeadersTuples,
        topic: str,
        partition: int,
        offset: int,
    ):
        """
        Add a new record to in-memory batch.
        """
        tp = (topic, partition)
        batch = self._batches.get(tp)
        if batch is None:
            batch = SinkBatch(topic=topic, partition=partition)
            self._batches[tp] = batch
        batch.append(
            value=value, key=key, timestamp=timestamp, headers=headers, offset=offset
        )

    def flush(self):
        """
        Flush accumulated batches to the destination and drop them afterward.
        """

        try:
            for (topic, partition), batch in self._batches.items():
                logger.debug(
                    f'Flushing sink "{self}" for partition "{topic}[{partition}]; '
                    f'total_records={batch.size}"'
                )
                # TODO: Some custom error handling may be needed here
                #   For now simply fail
                self.write(batch)
        finally:
            # Always drop batches after flushing
            self._batches.clear()

    def on_paused(self):
        """
        When the destination is already backpressured, drop the accumulated batches.
        """
        self._batches.clear()
