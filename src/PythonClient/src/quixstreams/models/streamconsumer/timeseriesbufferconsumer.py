import ctypes

from ...models.timeseriesbuffer import TimeseriesBuffer
from ...native.Python.QuixStreamsStreaming.Models.StreamConsumer.TimeseriesBufferConsumer import TimeseriesBufferConsumer as tsbci


class TimeseriesBufferConsumer(TimeseriesBuffer):
    """
    Represents a class for consuming data from a stream in a buffered manner.
    """

    def __init__(self, stream_consumer, net_pointer: ctypes.c_void_p = None):
        """
        Initializes a new instance of TimeseriesBufferConsumer.

        NOTE: Do not initialize this class manually,
        use StreamTimeseriesConsumer.create_buffer to create it.

        Args:
            stream_consumer: The Stream consumer which owns this timeseries buffer consumer.
            net_pointer: Pointer to an instance of a .net TimeseriesBufferConsumer.
                Defaults to None.

        Raises:
            Exception: If net_pointer is None.
        """
        if net_pointer is None:
            raise Exception("TimeseriesBufferConsumer is none")

        self._interop = tsbci(net_pointer)
        TimeseriesBuffer.__init__(self, stream_consumer, net_pointer)

    def get_net_pointer(self) -> ctypes.c_void_p:
        """
        Retrieves the pointer to the .net TimeseriesBufferConsumer instance.

        Returns:
            ctypes.c_void_p: The pointer to the .net TimeseriesBufferConsumer instance.
        """
        return self._interop.get_interop_ptr__()
