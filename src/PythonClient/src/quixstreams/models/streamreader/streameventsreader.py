from typing import List, Callable


from ...models.eventdefinition import EventDefinition
from ...models.eventdata import EventData

from ...native.Python.QuixSdkStreaming.Models.StreamReader.StreamEventsReader import StreamEventsReader as seri
from ...native.Python.InteropHelpers.InteropUtils import InteropUtils
from ...native.Python.InteropHelpers.ExternalTypes.System.Enumerable import Enumerable as ei
import ctypes
from ...helpers.nativedecorator import nativedecorator


@nativedecorator
class StreamEventsReader(object):

    def __init__(self, stream_reader, net_pointer: ctypes.c_void_p):
        """
            Initializes a new instance of StreamEventsReader.
            NOTE: Do not initialize this class manually, use StreamReader.events to access an instance of it

            Parameters:

            net_pointer (.net object): Pointer to an instance of a .net StreamEventsReader
        """
        if net_pointer is None:
            raise Exception("StreamEventsReader is none")

        self._interop = seri(net_pointer)

        self._stream_reader = stream_reader

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
    def on_read(self) -> Callable[['StreamReader', EventData], None]:
        """
        Gets the handler for when the stream receives event. First parameter is the stream the event is received for, second is the event.
        """
        return self._on_read

    @on_read.setter
    def on_read(self, value: Callable[['StreamReader', EventData], None]) -> None:
        """
        Sets the handler for when the stream receives event. First parameter is the stream the event is received for, second is the event.
        """
        self._on_read = value
        if self._on_read_ref is None:
            self._on_read_ref = self._interop.add_OnRead(self._on_read_wrapper)

    def _on_read_wrapper(self, stream_hptr, data_hptr):
        # To avoid unnecessary overhead and complication, we're using the stream instance we already have
        data = EventData(net_pointer=data_hptr)
        self._on_read(self._stream_reader, data)
        InteropUtils.free_hptr(stream_hptr)

    def _on_read_dispose(self):
        if self._on_read_ref is not None:
            self._interop.remove_OnRead(self._on_read_ref)
            self._on_read_ref = None
    # endregion on_read
    
    # region on_definitions_changed
    @property
    def on_definitions_changed(self) -> Callable[['StreamReader'], None]:
        """
        Gets the handler for when the stream definitions change. First parameter is the stream the event definitions changed for.
        """
        return self._on_definitions_changed

    @on_definitions_changed.setter
    def on_definitions_changed(self, value: Callable[['StreamReader'], None]) -> None:
        """
        Sets the handler for when the stream definitions change. First parameter is the stream the event definitions changed for.
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
    def definitions(self) -> List[EventDefinition]:
        """ Gets the latest set of event definitions """

        try:
            defs = self._interop.get_Definitions()

            asarray = ei.ReadReferences(defs)

            return [EventDefinition(hptr) for hptr in asarray]
        finally:
            InteropUtils.free_hptr(defs)
