import ctypes
import traceback
from typing import List, Callable

from ...helpers.nativedecorator import nativedecorator
from ...models.eventdata import EventData
from ...models.eventdefinition import EventDefinition
from ...native.Python.InteropHelpers.ExternalTypes.System.Enumerable import Enumerable as ei
from ...native.Python.InteropHelpers.InteropUtils import InteropUtils
from ...native.Python.QuixSdkStreaming.Models.StreamConsumer.EventDataReadEventArgs import EventDataReadEventArgs
from ...native.Python.QuixSdkStreaming.Models.StreamConsumer.EventDefinitionsChangedEventArgs import EventDefinitionsChangedEventArgs
from ...native.Python.QuixSdkStreaming.Models.StreamConsumer.StreamEventsConsumer import StreamEventsConsumer as seci


@nativedecorator
class StreamEventsConsumer(object):

    def __init__(self, topic_consumer, stream_consumer, net_pointer: ctypes.c_void_p):
        """
            Initializes a new instance of StreamEventsConsumer.
            NOTE: Do not initialize this class manually, use StreamConsumer.events to access an instance of it

            Parameters:

            topic_consumer: The input topic the stream belongs to
            stream_consumer: The stream the buffer is created for
            net_pointer (.net object): Pointer to an instance of a .net StreamEventsConsumer
        """
        if net_pointer is None:
            raise Exception("StreamEventsConsumer is none")

        self._interop = seci(net_pointer)
        self._topic_consumer = topic_consumer
        self._stream_consumer = stream_consumer

        # define events and their ref holder
        self._on_read = None
        self._on_read_ref = None  # keeping reference to avoid GC

        self._on_definitions_changed = None
        self._on_definitions_changed_ref = None  # keeping reference to avoid GC

    def _finalizerfunc(self):
        self._on_read_dispose()
        self._on_definitions_changed_dispose()

    # region on_read
    @property
    def on_read(self) -> Callable[['TopicConsumer', 'StreamConsumer', EventData], None]:
        """
        Gets the handler for when the stream receives event. First parameter is the topic, second is the stream the event is received for, third is the event.
        """
        return self._on_read

    @on_read.setter
    def on_read(self, value: Callable[['TopicConsumer', 'StreamConsumer', EventData], None]) -> None:
        """
        Sets the handler for when the stream receives event. First parameter is the topic, second is the stream the event is received for, third is the event.
        """
        self._on_read = value
        if self._on_read_ref is None:
            self._on_read_ref = self._interop.add_OnRead(self._on_read_wrapper)

    def _on_read_wrapper(self, stream_hptr, args_hptr):
        # To avoid unnecessary overhead and complication, we're using the stream instance we already have
        try:
            with (args := EventDataReadEventArgs(args_hptr)):
                data = EventData(net_pointer=args.get_Data())
                self._on_read(self._topic_consumer, self._stream_consumer, data)
            InteropUtils.free_hptr(stream_hptr)
        except:
            traceback.print_exc()

    def _on_read_dispose(self):
        if self._on_read_ref is not None:
            self._interop.remove_OnRead(self._on_read_ref)
            self._on_read_ref = None

    # endregion on_read

    # region on_definitions_changed
    @property
    def on_definitions_changed(self) -> Callable[['TopicConsumer', 'StreamConsumer'], None]:
        """
        Gets the handler for when the stream definitions change. First parameter is the topic, second is the stream the event definitions changed for.
        """
        return self._on_definitions_changed

    @on_definitions_changed.setter
    def on_definitions_changed(self, value: Callable[['TopicConsumer', 'StreamConsumer'], None]) -> None:
        """
        Sets the handler for when the stream definitions change. First parameter is the topic, second is the stream the event definitions changed for.
        """
        self._on_definitions_changed = value
        if self._on_definitions_changed_ref is None:
            self._on_definitions_changed_ref = self._interop.add_OnDefinitionsChanged(self._on_definitions_changed_wrapper)

    def _on_definitions_changed_wrapper(self, stream_hptr, args_hptr):
        # To avoid unnecessary overhead and complication, we're using the stream instance we already have
        try:
            with (args := EventDefinitionsChangedEventArgs(args_hptr)):
                self._on_definitions_changed(self._topic_consumer, self._stream_consumer)
            InteropUtils.free_hptr(stream_hptr)
        except:
            traceback.print_exc()

    def _on_definitions_changed_dispose(self):
        if self._on_definitions_changed_ref is not None:
            self._interop.remove_OnDefinitionsChanged(self._on_definitions_changed_ref)
            self._on_definitions_changed_ref = None

    # endregion on_definitions_changed

    @property
    def definitions(self) -> List[EventDefinition]:
        """ Gets the latest set of event definitions """

        try:
            defs = self._interop.get_Definitions()

            asarray = ei.ReadReferences(defs)

            return [EventDefinition(hptr) for hptr in asarray]
        finally:
            InteropUtils.free_hptr(defs)
