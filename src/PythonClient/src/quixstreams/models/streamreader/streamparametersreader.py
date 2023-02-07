from typing import List

from ...eventhook import EventHook

from ..parameterdefinition import ParameterDefinition
from ...models.parameterdataraw import ParameterDataRaw
from ...models.parameterdata import ParameterData
from ...models.parametersbufferconfiguration import ParametersBufferConfiguration
from ...models.streamreader.parametersbufferreader import ParametersBufferReader


from ...native.Python.QuixSdkStreaming.Models.StreamReader.StreamParametersReader import StreamParametersReader as spri
from ...native.Python.InteropHelpers.InteropUtils import InteropUtils
from ...native.Python.InteropHelpers.ExternalTypes.System.Enumerable import Enumerable as ei
from ...native.Python.InteropHelpers.ExternalTypes.System.Array import Array as ai
import ctypes
from ...helpers.nativedecorator import nativedecorator


@nativedecorator
class StreamParametersReader(object):

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
            Initializes a new instance of StreamParametersReader.
            NOTE: Do not initialize this class manually, use StreamReader.parameters to access an instance of it

            Parameters:

            net_pointer (.net object): Pointer to an instance of a .net StreamParametersReader
        """
        if net_pointer is None:
            raise Exception("StreamParametersReader is none")

        #TODOTEMP self._weakref = weakref.ref(self, lambda x: print("De-referenced StreamParametersReader " + str(net_pointer)))
        self._cfuncrefs = []  # exists to hold onto the references created by the interop layer to avoid GC'ing them
        self._interop = spri(net_pointer)
        self._buffers = []

        def _on_definitions_changed_net_handler():
            self.on_definitions_changed.fire()

        def _on_first_definitons_sub():
            ref = self._interop.add_OnDefinitionsChanged(_on_definitions_changed_net_handler)
            self._cfuncrefs.append(ref)

        def _on_last_definitons_unsub():
            # TODO fix unsub
            self._interop.remove_OnDefinitionsChanged(_on_definitions_changed_net_handler)

        self.on_definitions_changed = EventHook(_on_first_definitons_sub, _on_last_definitons_unsub, name="StreamParametersReader.on_definitions_changed")
        """
        Raised when the definitions have changed for the stream. Access "definitions" for latest set of parameter definitions 

        Has no arguments
        """

        def _on_read_net_handler(arg):
            self.on_read.fire(ParameterData(arg))

        def _on_first_sub():
            ref = self._interop.add_OnRead(_on_read_net_handler)
            self._cfuncrefs.append(ref)

        def _on_last_unsub():
            # TODO fix unsub
            self._interop.remove_OnRead(_on_read_net_handler)

        self.on_read = EventHook(_on_first_sub, _on_last_unsub, name="StreamParametersReader.on_read")
        """
        Event raised when data is available to read (without buffering) 
        This event does not use Buffers and data will be raised as they arrive without any processing.

        Has one argument of type ParameterData
        """


        def _on_read_raw_net_handler(arg):
            converted = ParameterDataRaw(arg)
            self.on_read_raw.fire(converted)

        def _on_first_raw_sub():
            ref = self._interop.add_OnReadRaw(_on_read_raw_net_handler)
            self._cfuncrefs.append(ref)

        def _on_last_raw_unsub():
            # TODO do unsign with previous handler
            self._interop.remove_OnReadRaw(_on_read_raw_net_handler)

        self.on_read_raw = EventHook(_on_first_raw_sub, _on_last_raw_unsub, name="StreamParametersReader.on_read_raw")
        """
        Event raised when data is available to read (without buffering) in raw transport format 
        This event does not use Buffers and data will be raised as they arrive without any processing. 

        Has one argument of type ParameterDataRaw 
        """

        def _on_read_df_net_handler(arg):
            with (pdrw := ParameterDataRaw(arg)):
                converted = pdrw.to_panda_frame()
            self.on_read_pandas.fire(converted)

        def _on_first_df_sub():
            ref = self._interop.add_OnReadRaw(_on_read_df_net_handler)
            self._cfuncrefs.append(ref)

        def _on_last_df_unsub():
            # TODO do unsign with previous handler
            self._interop.remove_OnReadRaw(_on_read_df_net_handler)

        self.on_read_pandas = EventHook(_on_first_df_sub, _on_last_df_unsub, name="StreamParametersReader.on_read_pandas")
        """
        Event raised when data is available to read (without buffering) in raw transport format 
        This event does not use Buffers and data will be raised as they arrive without any processing. 

        Has one argument of type ParameterDataRaw 
        """

    def _finalizerfunc(self):
        [buffer.dispose() for buffer in self._buffers]
        self._cfuncrefs = None

    @property
    def definitions(self) -> List[ParameterDefinition]:
        """ Gets the latest set of parameter definitions """

        try:
            defs_hptr = self._interop.get_Definitions()

            asarray = ei.ReadReferences(defs_hptr)

            return [ParameterDefinition(hptr) for hptr in asarray]
        finally:
            InteropUtils.free_hptr(defs_hptr)

    def create_buffer(self, *parameter_filter: str, buffer_configuration: ParametersBufferConfiguration = None) -> ParametersBufferReader:
        """
        Creates a new buffer for reading data according to the provided parameter_filter and buffer_configuration
        :param parameter_filter: 0 or more parameter identifier to filter as a whitelist. If provided, only these
            parameters will be available through this buffer
        :param buffer_configuration: an optional ParameterBufferConfiguration.

        :returns: a ParametersBufferReader which will raise new parameters read via .on_read event
        """

        actual_filters_uptr = None
        if parameter_filter is not None:
            filters = []
            for param_filter in parameter_filter:
                if isinstance(param_filter, ParametersBufferConfiguration):
                    buffer_configuration = param_filter
                    break
                filters.append(param_filter)
            if len(filters) > 0:
                actual_filters_uptr = ai.WriteStrings(filters)

        buffer = None
        if buffer_configuration is not None:
            buffer_config_ptr = buffer_configuration.get_net_pointer()
            buffer = ParametersBufferReader(self._interop.CreateBuffer(actual_filters_uptr, buffer_config_ptr))
        else:
            buffer = ParametersBufferReader(self._interop.CreateBuffer2(actual_filters_uptr))

        if actual_filters_uptr is not None:
            InteropUtils.free_uptr(actual_filters_uptr)

        self._buffers.append(buffer)
        return buffer

    def get_net_pointer(self) -> ctypes.c_void_p:
        return self._interop.get_interop_ptr__()
