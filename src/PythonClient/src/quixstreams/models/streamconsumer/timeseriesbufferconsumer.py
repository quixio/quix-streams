import ctypes

from ...models.timeseriesbuffer import TimeseriesBuffer

from ...native.Python.QuixSdkStreaming.Models.StreamConsumer.TimeseriesBufferConsumer import TimeseriesBufferConsumer as tsbci
#from ...helpers.nativedecorator import nativedecorator


#@nativedecorator #  TODO currently not decorating due to inheritance, but pending verification
class TimeseriesBufferConsumer(TimeseriesBuffer):
    """
        Class used to write to StreamProducer in a buffered manner
    """

    def __init__(self, topic_consumer, stream_consumer, net_pointer: ctypes.c_void_p = None):
        """
            Initializes a new instance of TimeseriesBufferConsumer.
            NOTE: Do not initialize this class manually, use StreamParametersConsumer.create_buffer to create it

            Parameters:
            topic_consumer: The input topic the stream belongs to
            stream_consumer: The stream the buffer is created for
            net_pointer: Pointer to an instance of a .net TimeseriesBufferConsumer
        """
        if net_pointer is None:
            raise Exception("TimeseriesBufferConsumer is none")

        self._interop = tsbci(net_pointer)
        TimeseriesBuffer.__init__(self, topic_consumer, stream_consumer, net_pointer)

    def get_net_pointer(self) -> ctypes.c_void_p:
        return self._interop.get_interop_ptr__()
