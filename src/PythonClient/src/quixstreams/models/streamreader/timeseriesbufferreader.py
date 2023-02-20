import ctypes

from ...models.timeseriesbuffer import TimeseriesBuffer

from ...native.Python.QuixSdkStreaming.Models.StreamReader.TimeseriesBufferReader import TimeseriesBufferReader as tsbri
#from ...helpers.nativedecorator import nativedecorator


#@nativedecorator #  TODO currently not decorating due to inheritance, but pending verification
class TimeseriesBufferReader(TimeseriesBuffer):
    """
        Class used to write to StreamWriter in a buffered manner
    """

    def __init__(self, stream_reader, net_pointer: ctypes.c_void_p = None):
        """
            Initializes a new instance of TimeseriesBufferReader.
            NOTE: Do not initialize this class manually, use StreamParametersReader.create_buffer to create it

            Parameters:

            net_pointer: Pointer to an instance of a .net TimeseriesBufferReader
        """
        if net_pointer is None:
            raise Exception("TimeseriesBufferReader is none")

        self._interop = tsbri(net_pointer)
        TimeseriesBuffer.__init__(self, stream_reader, net_pointer)

    def get_net_pointer(self) -> ctypes.c_void_p:
        return self._interop.get_interop_ptr__()
