from typing import List

from quixstreaming.eventhook import EventHook

from quixstreaming import __importnet, ParameterDefinition
import Quix.Sdk.Streaming

from quixstreaming.models.parameterdataraw import ParameterDataRaw
from quixstreaming.models.parameterdata import ParameterData
from quixstreaming.models.parametersbufferconfiguration import ParametersBufferConfiguration
from quixstreaming.models.streamreader.parametersbufferreader import ParametersBufferReader


class StreamParametersReader(object):

    def __init__(self, net_object: Quix.Sdk.Streaming.Models.StreamReader.StreamParametersReader):
        """
            Initializes a new instance of StreamParametersReader.
            NOTE: Do not initialize this class manually, use StreamReader.parameters to access an instance of it

            Parameters:

            net_object (.net object): The .net object representing a StreamParametersReader
        """
        if net_object is None:
            raise Exception("StreamParametersReader is none")
        self.__wrapped = net_object

        self.on_definitions_changed = EventHook(name="StreamParametersReader.on_definitions_changed")
        """
        Raised when the definitions have changed for the stream. Access "definitions" for latest set of parameter definitions 

        Has no arguments
        """

        def __on_definitions_changed_net_handler():
            self.on_definitions_changed.fire()
        self.__wrapped.OnDefinitionsChanged += __on_definitions_changed_net_handler



        def __on_read_net_handler(arg):
            self.on_read.fire(ParameterData(arg))

        def __on_first_sub():
            self.__wrapped.OnRead += __on_read_net_handler

        def __on_last_unsub():
            self.__wrapped.OnRead -= __on_read_net_handler

        self.on_read = EventHook(__on_first_sub, __on_last_unsub, name="StreamParametersReader.on_read")
        """
        Event raised when data is available to read (without buffering) 
        This event does not use Buffers and data will be raised as they arrive without any processing.

        Has one argument of type ParameterData
        """


        def __on_read_raw_net_handler(arg):
            converted = ParameterDataRaw(arg)
            self.on_read_raw.fire(converted)

        def __on_first_raw_sub():
            self.__wrapped.OnReadRaw += __on_read_raw_net_handler

        def __on_last_raw_unsub():
            self.__wrapped.OnReadRaw -= __on_read_raw_net_handler

        self.on_read_raw = EventHook(__on_first_raw_sub, __on_last_raw_unsub, name="StreamParametersReader.on_read_raw")
        """
        Event raised when data is available to read (without buffering) in raw transport format 
        This event does not use Buffers and data will be raised as they arrive without any processing. 

        Has one argument of type ParameterData in raw transport format 
        """



        def __on_read_pandas_net_handler(arg):
            converted = ParameterDataRaw(arg).to_panda_frame()
            self.on_read_pandas.fire(converted)

        def __on_first_raw_sub():
            self.__wrapped.OnReadRaw += __on_read_pandas_net_handler

        def __on_last_raw_unsub():
            self.__wrapped.OnReadRaw -= __on_read_pandas_net_handler

        self.on_read_pandas = EventHook(__on_first_raw_sub, __on_last_raw_unsub, name="StreamParametersReader.on_read_pandas")
        """
        Event raised when data is available to read (without buffering) in raw transport format 
        This event does not use Buffers and data will be raised as they arrive without any processing. 

        Has one argument of type ParameterData in raw transport format 
        """



    @property
    def definitions(self) -> List[ParameterDefinition]:
        """ Gets the latest set of parameter definitions """
        items = []
        for element in self.__wrapped.Definitions:
            item = ParameterDefinition(element)
            items.append(item)
        return items

    def create_buffer(self, *parameter_filter: str, buffer_configuration: ParametersBufferConfiguration = None) -> ParametersBufferReader:
        """
        Creates a new buffer for reading data according to the provided parameter_filter and buffer_configuration
        :param parameter_filter: 0 or more parameter identifier to filter as a whitelist. If provided, only these
            parameters will be available through this buffer
        :param buffer_configuration: an optional ParameterBufferConfiguration.

        :returns: a ParametersBufferReader which will raise new parameters read via .on_read event
        """

        actual_filters = []
        for param_filter in parameter_filter:
            if isinstance(param_filter, ParametersBufferConfiguration):
                buffer_configuration = param_filter
                break
            actual_filters.append(param_filter)

        buffer_config = None
        if buffer_configuration is not None:
            buffer_config = buffer_configuration.convert_to_net()
        dotnet_obj = self.__wrapped.CreateBuffer(buffer_config, actual_filters)
        return ParametersBufferReader(dotnet_obj)