import ctypes
import traceback
from typing import List, Tuple

from ..helpers.nativedecorator import nativedecorator
from ..native.Python.InteropHelpers.InteropUtils import InteropUtils
from ..native.Python.InteropHelpers.ExternalTypes.System.Array import Array as ai
from ..native.Python.QuixStreamsStreaming.States.StreamDictionaryState import StreamDictionaryState as sdsi

from ..models.netdict import NetDict
from ..state.statevalue import StateValue

from typing import TypeVar, Generic, Callable

StreamStateType = TypeVar('StreamStateType')


@nativedecorator
class DictStreamState(Generic[StreamStateType]):
    """
    Represents a state container that stores key-value pairs with the ability to flush changes to a specified storage.
    """

    def __init__(self, net_pointer: ctypes.c_void_p, state_type: StreamStateType, default_value_factory: Callable[[str], StreamStateType]):
        """
        Initializes a new instance of DictStreamState.

        NOTE: Do not initialize this class manually, use StreamStateManager.get_dict_state

        Args:
            net_pointer: The .net object representing a DictStreamState.
            state_type: The type of the state
            default_value_factory: The default value factory to create value when the key is not yet present in the state
        """

        if net_pointer is None:
            raise Exception("DictStreamState is none")

        if state_type is None:
            raise Exception('state_type must be specified')

        self._interop = sdsi(net_pointer)
        self._default_value_factory = default_value_factory
        self._type = state_type

        def value_converter(val):
            return StateValue(val)

        def convert_val_to_python(val_hptr : ctypes.c_void_p) -> StreamStateType:
            if val_hptr is None:
                return None
            return StateValue(val_hptr).value

        def convert_val_from_python(val: StreamStateType) -> ctypes.c_void_p:
            if val is None:
                return None
            return value_converter(val).get_net_pointer()

        def convert_val_to_python_list(arr_uptr: ctypes.c_void_p) -> List[StateValue]:
            hptrs = ai.ReadPointers(arr_uptr)
            return [StateValue(hptr) for hptr in hptrs]

        self._in_memory = {}
        self._underlying = NetDict(net_pointer=net_pointer,
                                   key_converter_to_python=InteropUtils.ptr_to_utf8,
                                   key_converter_from_python=InteropUtils.utf8_to_ptr,
                                   val_converter_to_python=convert_val_to_python,
                                   val_converter_from_python=convert_val_from_python,
                                   key_converter_to_python_list=ai.ReadStrings,
                                   val_converter_to_python_list=convert_val_to_python_list,
                                   self_dispose=False)

        # Define events and their reference holders
        self._on_flushed = None
        self._on_flushed_refs = None  # Keeping references to avoid garbage collection

        self._on_flushing = None
        self._on_flushing_refs = None  # Keeping references to avoid garbage collection

        # Check if type is immutable, because it needs special handling. Content could change without StreamState being
        # notified
        self._immutable = self._type in (int, float, bool, complex, str, bytes, bytearray, range)
        self._on_flushing_internal = None
        if not self._immutable:
            def on_flushing_internal():
                for index, (key, val) in enumerate(self._in_memory.items()):
                    self._underlying[key] = val

            self._on_flushing_internal = on_flushing_internal
            self.on_flushing = None  # this will subscribe to the event and invoke the internal

    def _finalizerfunc(self):
        self._on_flushed_dispose()
        self._on_flushing_dispose()

    @property
    def type(self) -> type:
        """
        Gets the type of the StreamState

        Returns:
            StreamStateType: type of the state
        """
        return self._type

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
        self._on_flushed_dispose()

        if self._on_flushed is None:
            return

        self._on_flushed_refs = self._interop.add_OnFlushed(self._on_flushed_wrapper)

    def _on_flushed_wrapper(self, sender_hptr, args_hptr):
        try:
            self._on_flushed()
        except:
            traceback.print_exc()
        finally:
            InteropUtils.free_hptr(sender_hptr)
            InteropUtils.free_hptr(args_hptr)

    def _on_flushed_dispose(self):
        if self._on_flushed_refs is not None:
            self._interop.remove_OnFlushed(self._on_flushed_refs[0])
            self._on_flushed_refs = None

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
        self._on_flushing_dispose()

        if self._on_flushing is None and self._on_flushing_internal is None:
            return

        self._on_flushing_refs = self._interop.add_OnFlushing(self._on_flushing_wrapper)

    def _on_flushing_wrapper(self, sender_hptr, args_hptr):
        try:
            if self._on_flushing is not None:
                self._on_flushing()
            if self._on_flushing_internal is not None:
                self._on_flushing_internal()
        except:
            traceback.print_exc()
        finally:
            InteropUtils.free_hptr(sender_hptr)
            InteropUtils.free_hptr(args_hptr)


    def _on_flushing_dispose(self):
        if self._on_flushing_refs is not None:
            self._interop.remove_OnFlushing(self._on_flushing_refs[0])
            self._on_flushing_refs = None

    # End region on_flushing

    def flush(self):
        """
        Flushes the changes made to the in-memory state to the specified storage.
        """
        self._interop.Flush()

    def reset(self):
        """
        Reset the state to before in-memory modifications
        """
        self._interop.Reset()

    # region dictionary interface
    def __getitem__(self, key: str) -> StreamStateType:
        try:
            return self._in_memory[key]
        except KeyError:
            try:
                value = self._underlying[key]
            except KeyError:
                if self._default_value_factory is None:
                    raise

                value = self._default_value_factory(key)
                self[key] = value
                return value

            self._in_memory[key] = value
            return value

    def __contains__(self, item: str) -> bool:
        if item in self._in_memory:
            return True

        return item in self._underlying

    def __str__(self) -> str:
        # Not a guarantee we have the whole list in memory, therefor use underlying
        return str(self._underlying)

    def __len__(self) -> int:
        # Not a guarantee we have the whole list in memory, therefor use underlying
        return len(self._underlying)

    def keys(self) -> List[str]:
        # Not a guarantee we have the whole list in memory, therefor use underlying
        return self._underlying.keys()

    def values(self) -> List[StreamStateType]:
        # Not a guarantee we have the whole list in memory, therefor use underlying
        self._underlying.values()

    def __iter__(self):
        # requirement for enumerate(self), else the enumeration doesn't work as expected
        # better performance than iterating using __getitem__ and can be terminated without full materialization
        for key in self.keys():
            yield key, self[key]

    def items(self) -> List[Tuple[str, StreamStateType]]:
        item_list: list = []
        for index, (key, value) in enumerate(self):
            item_list.append((key, value))
        return item_list

    def __setitem__(self, key: str, value: StreamStateType):
        self._in_memory[key] = value
        self._underlying[key] = value

    def __delitem__(self, key: str):
        del self._underlying[key]
        del self._in_memory[key]

    def update(self, key: str, item: StreamStateType):
        self[key] = item

    def clear(self):
        self._in_memory.clear()
        self._underlying.clear()

    def pop(self, key: str):
        val = self[key]
        del self[key]
        return val
    # end region dictionary interface

