import ctypes
import traceback
from typing import Callable, List

from .helpers.nativedecorator import nativedecorator
from .native.Python.InteropHelpers.InteropUtils import InteropUtils
from .native.Python.InteropHelpers.ExternalTypes.System.Array import Array as ai
from .native.Python.QuixStreamsStreaming.StreamState import StreamState as ssi

from .models.netdict import NetDict
from .state.statevalue import StateValue


@nativedecorator
class StreamState(NetDict):
    """
    Represents a dictionary-like storage of key-value pairs with a specific topic and storage name.
    """

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
        Initializes a new instance of StreamState.

        NOTE: Do not initialize this class manually, use StreamStateManager.get_state

        Args:
            net_pointer: The .net object representing a StreamState.
        """

        if net_pointer is None:
            raise Exception("StreamState is none")

        self._interop = ssi(net_pointer)

        def convert_val_to_python(val_hptr : ctypes.c_void_p) -> StateValue:
            if val_hptr is None:
                return None
            return StateValue(val_hptr)

        def convert_val_from_python(val: StateValue) -> ctypes.c_void_p:
            if val is None:
                return None
            return val.get_net_pointer()

        def convert_val_to_python_list(arr_uptr: ctypes.c_void_p) -> List[StateValue]:
            hptrs = ai.ReadPointers(arr_uptr)
            return [StateValue(hptr) for hptr in hptrs]

        NetDict.__init__(self,
                         net_pointer=net_pointer,
                         key_converter_to_python=InteropUtils.ptr_to_utf8,
                         key_converter_from_python=InteropUtils.utf8_to_ptr,
                         val_converter_to_python=convert_val_to_python,
                         val_converter_from_python=convert_val_from_python,
                         key_converter_to_python_list=ai.ReadStrings,
                         val_converter_to_python_list=convert_val_to_python_list)

        # Define events and their reference holders
        self._on_flushed = None
        self._on_flushed_ref = None  # Keeping reference to avoid garbage collection

        self._on_flushing = None
        self._on_flushing_ref = None  # Keeping reference to avoid garbage collection

    def _finalizerfunc(self):
        self._on_flushed_dispose()
        self._on_flushing_dispose()

    # Region on_flushed
    @property
    def on_flushed(self) -> Callable[[], None]:
        """
        Gets the handler for when flush operation is completed.

        Returns:
            Callable[[], None]: The event handler for after flush.
        """
        return self._on_flushed

    @on_flushed.setter
    def on_flushed(self, value: Callable[[], None]) -> None:
        """
        Sets the handler for when flush operation is completed.

        Args:
            value: The parameterless callback to invoke
        """

        self._on_flushed = value
        if self._on_flushed_ref is None:
            self._on_flushed_ref = self._interop.add_OnFlushed(self._on_flushed_wrapper)

    def _on_flushed_wrapper(self, sender_hptr, args_hptr):
        try:
            self._on_flushed(self._stream_consumer)
        except:
            traceback.print_exc()
        finally:
            InteropUtils.free_hptr(sender_hptr)
            InteropUtils.free_hptr(args_hptr)

    def _on_flushed_dispose(self):
        if self._on_flushed_ref is not None:
            self._interop.remove_OnFlushed(self._on_flushed_ref)
            self._on_flushed_ref = None

    # End region on_flushed

    # Region on_flushing
    @property
    def on_flushing(self) -> Callable[[], None]:
        """
        Gets the handler for when flush operation begins.

        Returns:
            Callable[[], None]: The event handler for after flush.
        """
        return self._on_flushing

    @on_flushing.setter
    def on_flushing(self, value: Callable[[], None]) -> None:
        """
        Sets the handler for when flush operation begins.

        Args:
            value: The parameterless callback to invoke
        """

        self._on_flushing = value
        if self._on_flushing_ref is None:
            self._on_flushing_ref = self._interop.add_OnFlushing(self._on_flushing_wrapper)

    def _on_flushing_wrapper(self, sender_hptr, args_hptr):
        try:
            self._on_flushing(self._stream_consumer)
        except:
            traceback.print_exc()
        finally:
            InteropUtils.free_hptr(sender_hptr)
            InteropUtils.free_hptr(args_hptr)

    def _on_flushing_dispose(self):
        if self._on_flushing_ref is not None:
            self._interop.remove_OnFlushing(self._on_flushing_ref)
            self._on_flushing_ref = None

    # End region on_flushing
