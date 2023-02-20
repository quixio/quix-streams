from typing import List, Callable

import pandas

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

    def __init__(self, stream_reader, net_pointer: ctypes.c_void_p):
        """
            Initializes a new instance of StreamParametersReader.
            NOTE: Do not initialize this class manually, use StreamReader.parameters to access an instance of it

            Parameters:

            net_pointer (.net object): Pointer to an instance of a .net StreamParametersReader
        """
        if net_pointer is None:
            raise Exception("StreamParametersReader is none")

        self._interop = spri(net_pointer)
        self._buffers = []

        self._stream_reader = stream_reader

        # define events and their ref holder
        self._on_read = None
        self._on_read_ref = None  # keeping reference to avoid GC

        self._on_read_raw = None
        self._on_read_raw_ref = None  # keeping reference to avoid GC

        self._on_read_dataframe = None
        self._on_read_dataframe_ref = None  # keeping reference to avoid GC

        self._on_definitions_changed = None
        self._on_definitions_changed_ref = None  # keeping reference to avoid GC

    def _finalizerfunc(self):
        [buffer.dispose() for buffer in self._buffers]
        self._on_read_dispose()
        self._on_read_raw_dispose()
        self._on_read_dataframe_dispose()
        self._on_definitions_changed_dispose()

    # region on_read
    @property
    def on_read(self) -> Callable[['StreamReader', ParameterData], None]:
        """
        Gets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in ParameterData format.
        """
        return self._on_read

    @on_read.setter
    def on_read(self, value: Callable[['StreamReader', ParameterData], None]) -> None:
        """
        Sets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in ParameterData format.
        """
        self._on_read = value
        if self._on_read_ref is None:
            self._on_read_ref = self._interop.add_OnRead(self._on_read_wrapper)

    def _on_read_wrapper(self, stream_hptr, data_hptr):
        # To avoid unnecessary overhead and complication, we're using the stream instance we already have
        self._on_read(self._stream_reader, ParameterData(data_hptr))
        InteropUtils.free_hptr(stream_hptr)

    def _on_read_dispose(self):
        if self._on_read_ref is not None:
            self._interop.remove_OnRead(self._on_read_ref)
            self._on_read_ref = None
    # endregion on_read

    # region on_read_raw
    @property
    def on_read_raw(self) -> Callable[['StreamReader', ParameterDataRaw], None]:
        """
        Gets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in ParameterDataRaw format.
        """
        return self._on_read_raw

    @on_read_raw.setter
    def on_read_raw(self, value: Callable[['StreamReader', ParameterDataRaw], None]) -> None:
        """
        Sets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in ParameterDataRaw format.
        """
        self._on_read_raw = value
        if self._on_read_raw_ref is None:
            self._on_read_raw_ref = self._interop.add_OnReadRaw(self._on_read_raw_wrapper)

    def _on_read_raw_wrapper(self, stream_hptr, data_hptr):
        # To avoid unnecessary overhead and complication, we're using the stream instance we already have
        self._on_read_raw(self._stream_reader, ParameterDataRaw(data_hptr))
        InteropUtils.free_hptr(stream_hptr)

    def _on_read_raw_dispose(self):
        if self._on_read_raw_ref is not None:
            self._interop.remove_OnReadRaw(self._on_read_raw_ref)
            self._on_read_raw_ref = None
    # endregion on_read_raw

    # region on_read_dataframe
    @property
    def on_read_dataframe(self) -> Callable[['StreamReader', pandas.DataFrame], None]:
        """
        Gets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in Pandas' DataFrame format.
        """
        return self._on_read_dataframe

    @on_read_dataframe.setter
    def on_read_dataframe(self, value: Callable[['StreamReader', pandas.DataFrame], None]) -> None:
        """
        Sets the handler for when the stream receives data. First parameter is the stream the data is received for, second is the data in Pandas' DataFrame format.
        """
        self._on_read_dataframe = value
        if self._on_read_dataframe_ref is None:
            self._on_read_dataframe_ref = self._interop.add_OnReadRaw(self._on_read_dataframe_wrapper)

    def _on_read_dataframe_wrapper(self, stream_hptr, data_hptr):
        # To avoid unnecessary overhead and complication, we're using the stream instance we already have
        pdr = ParameterDataRaw(data_hptr)
        pdf = pdr.to_panda_dataframe()
        pdr.dispose()
        self._on_read_dataframe(self._stream_reader, pdf)
        InteropUtils.free_hptr(stream_hptr)

    def _on_read_dataframe_dispose(self):
        if self._on_read_dataframe_ref is not None:
            self._interop.remove_OnReadRaw(self._on_read_dataframe_ref)
            self._on_read_dataframe_ref = None
    # endregion on_read_dataframe

    # region on_definitions_changed
    @property
    def on_definitions_changed(self) -> Callable[['StreamReader'], None]:
        """
        Gets the handler for when the stream definitions change. First parameter is the stream the parameter definitions changed for.
        """
        return self._on_definitions_changed

    @on_definitions_changed.setter
    def on_definitions_changed(self, value: Callable[['StreamReader'], None]) -> None:
        """
        Sets the handler for when the stream definitions change. First parameter is the stream the parameter definitions changed for.
        """
        self._on_definitions_changed = value
        if self._on_definitions_changed_ref is None:
            self._on_definitions_changed_ref = self._interop.add_OnDefinitionsChanged(self._on_definitions_changed_wrapper)

    def _on_definitions_changed_wrapper(self, stream_hptr, args_ptr):
        # To avoid unnecessary overhead and complication, we're using the stream instance we already have
        self._on_definitions_changed(self._stream_reader)
        InteropUtils.free_hptr(stream_hptr)
        InteropUtils.free_hptr(args_ptr)

    def _on_definitions_changed_dispose(self):
        if self._on_definitions_changed_ref is not None:
            self._interop.remove_OnDefinitionsChanged(self._on_definitions_changed_ref)
            self._on_definitions_changed_ref = None
    # endregion on_definitions_changed

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
            buffer = ParametersBufferReader(self._stream_reader, self._interop.CreateBuffer(actual_filters_uptr, buffer_config_ptr))
        else:
            buffer = ParametersBufferReader(self._stream_reader, self._interop.CreateBuffer2(actual_filters_uptr))

        self._buffers.append(buffer)
        return buffer

    def get_net_pointer(self) -> ctypes.c_void_p:
        return self._interop.get_interop_ptr__()
