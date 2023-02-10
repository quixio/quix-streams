import ctypes

from ...models.parametersbuffer import ParametersBuffer

from ...native.Python.QuixSdkStreaming.Models.StreamReader.ParametersBufferReader import ParametersBufferReader as pbri
#from ...helpers.nativedecorator import nativedecorator


#@nativedecorator #  TODO currently not decorating due to inheritance, but pending verification
class ParametersBufferReader(ParametersBuffer):
    """
        Class used to write to StreamWriter in a buffered manner
    """

    def __init__(self, stream_reader, net_pointer: ctypes.c_void_p = None):
        """
            Initializes a new instance of ParametersBufferReader.
            NOTE: Do not initialize this class manually, use StreamParametersReader.create_buffer to create it

            Parameters:

            net_pointer: Pointer to an instance of a .net ParametersBufferReader
        """
        if net_pointer is None:
            raise Exception("ParametersBufferReader is none")

        self._interop = pbri(net_pointer)
        ParametersBuffer.__init__(self, stream_reader, net_pointer)

    def get_net_pointer(self) -> ctypes.c_void_p:
        return self._interop.get_interop_ptr__()
