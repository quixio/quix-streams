import traceback
from typing import List, Callable

import pandas

from ..parameterdefinition import ParameterDefinition
from ...models.timeseriesdataraw import TimeseriesDataRaw
from ...models.timeseriesdata import TimeseriesData
from ...models.timeseriesbufferconfiguration import TimeseriesBufferConfiguration
from ...models.streamconsumer.timeseriesbufferconsumer import TimeseriesBufferConsumer


from ...native.Python.QuixSdkStreaming.Models.StreamConsumer.StreamParametersConsumer import StreamParametersConsumer as spci
from ...native.Python.QuixSdkStreaming.Models.StreamConsumer.TimeseriesDataReadEventArgs import TimeseriesDataReadEventArgs
from ...native.Python.QuixSdkStreaming.Models.StreamConsumer.TimeseriesDataRawReadEventArgs import TimeseriesDataRawReadEventArgs
from ...native.Python.QuixSdkStreaming.Models.StreamConsumer.ParameterDefinitionsChangedEventArgs import ParameterDefinitionsChangedEventArgs
from ...native.Python.InteropHelpers.InteropUtils import InteropUtils
from ...native.Python.InteropHelpers.ExternalTypes.System.Enumerable import Enumerable as ei
from ...native.Python.InteropHelpers.ExternalTypes.System.Array import Array as ai
import ctypes
from ...helpers.nativedecorator import nativedecorator


@nativedecorator
class StreamParametersConsumer(object):

    def __init__(self, topic_consumer, stream_reader, net_pointer: ctypes.c_void_p):
        """
            Initializes a new instance of StreamParametersConsumer.
            NOTE: Do not initialize this class manually, use StreamConsumer.parameters to access an instance of it

            Parameters:

            topic_consumer: The input topic the stream belongs to
            stream_reader: The stream the buffer is created for
            net_pointer (.net object): Pointer to an instance of a .net StreamParametersConsumer
        """
        if net_pointer is None:
            raise Exception("StreamParametersConsumer is none")

        self._interop = spci(net_pointer)
        self._buffers = []

        self._topic_consumer = topic_consumer
        self._stream_reader = stream_reader

        # define events and their ref holder
        self._on_read = None
        self._on_read_ref = None  # keeping reference to avoid GC

        self._on_raw_read = None
        self._on_raw_read_ref = None  # keeping reference to avoid GC

        self._on_read_dataframe = None
        self._on_read_dataframe_ref = None  # keeping reference to avoid GC

        self._on_definitions_changed = None
        self._on_definitions_changed_ref = None  # keeping reference to avoid GC

    def _finalizerfunc(self):
        [buffer.dispose() for buffer in self._buffers]
        self._on_read_dispose()
        self._on_raw_read_dispose()
        self._on_read_dataframe_dispose()
        self._on_definitions_changed_dispose()

    # region on_read
    @property
    def on_read(self) -> Callable[['TopicConsumer', 'StreamConsumer', TimeseriesData], None]:
        """
        Gets the handler for when the stream receives data. First parameter is the topic, second is the stream the data is received for, third is the data in TimeseriesData format.
        """
        return self._on_read

    @on_read.setter
    def on_read(self, value: Callable[['TopicConsumer', 'StreamConsumer', TimeseriesData], None]) -> None:
        """
        Sets the handler for when the stream receives data. First parameter is the topic, second is the stream the data is received for, third is the data in TimeseriesData format.
        """
        self._on_read = value
        if self._on_read_ref is None:
            self._on_read_ref = self._interop.add_OnRead(self._on_read_wrapper)

    def _on_read_wrapper(self, stream_hptr, args_hptr):
        # To avoid unnecessary overhead and complication, we're using the stream instance we already have
        try:
            with (args := TimeseriesDataReadEventArgs(args_hptr)):
                self._on_read(self._topic_consumer, self._stream_reader, TimeseriesData(args.get_Data()))
            InteropUtils.free_hptr(stream_hptr)
        except:
            traceback.print_exc()

    def _on_read_dispose(self):
        if self._on_read_ref is not None:
            self._interop.remove_OnRead(self._on_read_ref)
            self._on_read_ref = None
    # endregion on_read

    # region on_raw_read
    @property
    def on_raw_read(self) -> Callable[['TopicConsumer', 'StreamConsumer', TimeseriesDataRaw], None]:
        """
        Gets the handler for when the stream receives data. First parameter is the topic, second is the stream the data is received for, third is the data in TimeseriesDataRaw format.
        """
        return self._on_raw_read

    @on_raw_read.setter
    def on_raw_read(self, value: Callable[['TopicConsumer', 'StreamConsumer', TimeseriesDataRaw], None]) -> None:
        """
        Sets the handler for when the stream receives data. First parameter is the topic, second is the stream the data is received for, third is the data in TimeseriesDataRaw format.
        """
        self._on_raw_read = value
        if self._on_raw_read_ref is None:
            self._on_raw_read_ref = self._interop.add_OnRawRead(self._on_raw_read_wrapper)

    def _on_raw_read_wrapper(self, stream_hptr, args_hptr):
        # To avoid unnecessary overhead and complication, we're using the stream instance we already have
        try:
            with (args := TimeseriesDataRawReadEventArgs(args_hptr)):
                self._on_raw_read(self._topic_consumer, self._stream_reader, TimeseriesDataRaw(args.get_Data()))
            InteropUtils.free_hptr(stream_hptr)
        except:
            traceback.print_exc()

    def _on_raw_read_dispose(self):
        if self._on_raw_read_ref is not None:
            self._interop.remove_OnRawRead(self._on_raw_read_ref)
            self._on_raw_read_ref = None
    # endregion on_raw_read

    # region on_read_dataframe
    @property
    def on_read_dataframe(self) -> Callable[['TopicConsumer', 'StreamConsumer', pandas.DataFrame], None]:
        """
        Gets the handler for when the stream receives data. First parameter is the topic, second is the stream the data is received for, third is the data in Pandas' DataFrame format.
        """
        return self._on_read_dataframe

    @on_read_dataframe.setter
    def on_read_dataframe(self, value: Callable[['TopicConsumer', 'StreamConsumer', pandas.DataFrame], None]) -> None:
        """
        Sets the handler for when the stream receives data. First parameter is the topic, second is the stream the data is received for, third is the data in Pandas' DataFrame format.
        """
        self._on_read_dataframe = value
        if self._on_read_dataframe_ref is None:
            self._on_read_dataframe_ref = self._interop.add_OnRawRead(self._on_read_dataframe_wrapper)

    def _on_read_dataframe_wrapper(self, stream_hptr, args_hptr):
        # To avoid unnecessary overhead and complication, we're using the stream instance we already have
        try:
            with (args := TimeseriesDataRawReadEventArgs(args_hptr)):
                pdr = TimeseriesDataRaw(args.get_Data())
                pdf = pdr.to_panda_dataframe()
                pdr.dispose()
                self._on_read_dataframe(self._topic_consumer, self._stream_reader, pdf)
            InteropUtils.free_hptr(stream_hptr)
        except:
            traceback.print_exc()

    def _on_read_dataframe_dispose(self):
        if self._on_read_dataframe_ref is not None:
            self._interop.remove_OnRawRead(self._on_read_dataframe_ref)
            self._on_read_dataframe_ref = None
    # endregion on_read_dataframe

    # region on_definitions_changed
    @property
    def on_definitions_changed(self) -> Callable[['TopicConsumer', 'StreamConsumer'], None]:
        """
        Gets the handler for when the stream definitions change. First parameter is the topic, second is the stream the parameter definitions changed for.
        """
        return self._on_definitions_changed

    @on_definitions_changed.setter
    def on_definitions_changed(self, value: Callable[['TopicConsumer', 'StreamConsumer'], None]) -> None:
        """
        Sets the handler for when the stream definitions change. First parameter is the topic, second is the stream the parameter definitions changed for.
        """
        self._on_definitions_changed = value
        if self._on_definitions_changed_ref is None:
            self._on_definitions_changed_ref = self._interop.add_OnDefinitionsChanged(self._on_definitions_changed_wrapper)

    def _on_definitions_changed_wrapper(self, stream_hptr, args_hptr):
        # To avoid unnecessary overhead and complication, we're using the stream instance we already have
        try:
            with (args := ParameterDefinitionsChangedEventArgs(args_hptr)):
                self._on_definitions_changed(self._topic_consumer, self._stream_reader)
                InteropUtils.free_hptr(stream_hptr)
        except:
            traceback.print_exc()

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

    def create_buffer(self, *parameter_filter: str, buffer_configuration: TimeseriesBufferConfiguration = None) -> TimeseriesBufferConsumer:
        """
        Creates a new buffer for reading data according to the provided parameter_filter and buffer_configuration
        :param parameter_filter: 0 or more parameter identifier to filter as a whitelist. If provided, only these
            parameters will be available through this buffer
        :param buffer_configuration: an optional TimeseriesBufferConfiguration.

        :returns: a TimeseriesBufferConsumer which will raise new parameters read via .on_read event
        """

        actual_filters_uptr = None
        if parameter_filter is not None:
            filters = []
            for param_filter in parameter_filter:
                if isinstance(param_filter, TimeseriesBufferConfiguration):
                    buffer_configuration = param_filter
                    break
                filters.append(param_filter)
            if len(filters) > 0:
                actual_filters_uptr = ai.WriteStrings(filters)

        buffer = None
        if buffer_configuration is not None:
            buffer_config_ptr = buffer_configuration.get_net_pointer()
            buffer = TimeseriesBufferConsumer(self._topic_consumer, self._stream_reader, self._interop.CreateBuffer(actual_filters_uptr, buffer_config_ptr))
        else:
            buffer = TimeseriesBufferConsumer(self._topic_consumer, self._stream_reader, self._interop.CreateBuffer2(actual_filters_uptr))

        self._buffers.append(buffer)
        return buffer

    def get_net_pointer(self) -> ctypes.c_void_p:
        return self._interop.get_interop_ptr__()
