from typing import Optional, Union

from quixstreams.sinks import (
    BatchingSink,
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
    SinkBackpressureError,
    SinkBatch,
)

from .destinations import Destination, LocalDestination
from .formats import Format, FormatName, resolve_format

__all__ = ("FileSink",)


class FileSink(BatchingSink):
    """A sink that writes data batches to files using configurable formats and
    destinations.

    The sink groups messages by their topic and partition, ensuring data from the
    same source is stored together. Each batch is serialized using the specified
    format (e.g., JSON, Parquet) before being written to the configured
    destination.

    The destination determines the storage location and write behavior. By default,
    it uses LocalDestination for writing to the local filesystem, but can be
    configured to use other storage backends (e.g., cloud storage).
    """

    def __init__(
        self,
        directory: str = "",
        format: Union[FormatName, Format] = "json",
        destination: Optional[Destination] = None,
        client_connect_success_cb: Optional[ClientConnectSuccessCallback] = None,
        client_connect_failure_cb: Optional[ClientConnectFailureCallback] = None,
    ) -> None:
        """Initialize the FileSink with the specified configuration.

        :param directory: Base directory path for storing files. Defaults to
            current directory.
        :param format: Data serialization format, either as a string
            ("json", "parquet") or a Format instance.
        :param destination: Storage destination handler. Defaults to
            LocalDestination if not specified.
        :param client_connect_success_cb: An optional callback made after successful
            client authentication, primarily for additional logging.
        :param client_connect_failure_cb: An optional callback made after failed
            client authentication (which should raise an Exception).
            Callback should accept the raised Exception as an argument.
            Callback must resolve (or propagate/re-raise) the Exception.
        """
        self._format = resolve_format(format)
        self._destination = destination or LocalDestination()
        self._destination.set_directory(directory)
        self._destination.set_extension(self._format)
        super().__init__(
            client_connect_success_cb=client_connect_success_cb,
            client_connect_failure_cb=client_connect_failure_cb,
        )

    def setup_client(self) -> Destination:
        self._destination.connect()
        return self._destination

    def write(self, batch: SinkBatch) -> None:
        """Write a batch of data using the configured format and destination.

        The method performs the following steps:
        1. Serializes the batch data using the configured format
        2. Writes the serialized data to the destination
        3. Handles any write failures by raising a backpressure error

        :param batch: The batch of data to write.
        :raises SinkBackpressureError: If the write operation fails, indicating
            that the sink needs backpressure with a 5-second retry delay.
        """
        data = self._format.serialize(batch)

        try:
            self._destination.write(data, batch)
        except Exception as e:
            raise SinkBackpressureError(
                retry_after=5.0,
                topic=batch.topic,
                partition=batch.partition,
            ) from e
