from quixstreaming import __importnet
import Quix.Sdk.Streaming

from quixstreaming.models.parametersbuffer import ParametersBuffer


class ParametersBufferReader(ParametersBuffer):
    """
        Class used to write to StreamWriter in a buffered manner
    """

    def __init__(self, net_object: Quix.Sdk.Streaming.Models.StreamReader.ParametersBufferReader):
        """
            Initializes a new instance of ParametersBufferReader.
            NOTE: Do not initialize this class manually, use StreamParametersReader.create_buffer to create it

            Parameters:

            net_object (.net object): The .net object representing a ParametersBufferReader
        """
        if net_object is None:
            raise Exception("StreamEventsWriter is none")
        self.__wrapped = net_object
        ParametersBuffer.__init__(self, net_object)

    def dispose(self):
        """
        Disposes the underlying objects. Call once the buffer is not required
        """
        self.__wrapped.Dispose()
