import ctypes
from typing import Any, Dict

from .. import TimeseriesDataRaw
from ..helpers.nativedecorator import nativedecorator
from ..native.Python.InteropHelpers.InteropUtils import InteropUtils
from ..native.Python.QuixStreamsTelemetry.Models.StreamPackage import StreamPackage as spi
from ..native.Python.SystemPrivateCoreLib.System.Type import Type as NetType


@nativedecorator
class StreamPackage(object):
    """
        Default model implementation for non-typed message packages of the Telemetry layer. It holds a value and its type.
    """

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
        Initializes a new instance of StreamPackage.

        Note:
            Do not initialize this class manually. Will be initialized by StreamConsumer.on_package_received.

        Args:
            net_pointer: Pointer to an instance of a .net StreamPackage.
        """

        self._interop = spi(net_pointer)
        with (nettype := (NetType(self._interop.get_Type()))):
            self.type = nettype.get_FullName()
            """Type of the content value"""

            self.value: Any = None
            """Content value of the package"""

            val_hptr = self._interop.get_Value()

            if self.type == "QuixStreams.Process.Models.StreamProperties":
                # todo
                pass
            elif self.type == "QuixStreams.Process.Models.ParameterDefinitions":
                # todo
                pass
            elif self.type == "QuixStreams.Process.Models.EventDefinitions":
                # todo
                pass
            elif self.type == "QuixStreams.Process.Models.StreamEnd":
                self.value = InteropUtils.hptr_to_uptr(val_hptr)
                pass
            elif self.type == "QuixStreams.Process.models.timeseriesdataRaw":
                self.value = TimeseriesDataRaw(val_hptr)
            elif self.type == "QuixStreams.Process.Models.EventDataRaw[]":
                pass
            elif self.type == "QuixStreams.Process.Models.EventDataRaw":
                pass

    @property
    def transport_context(self) -> Dict[str, str]:
        """Context holder for package when transporting through the pipeline."""
        return {}
        raise NotImplemented("TODO")

    def to_json(self) -> str:
        """
        Serialize the package into JSON.

        Returns:
            str: The serialized JSON string of the package.
        """
        return self._interop.ToJson()

    def get_net_pointer(self) -> ctypes.c_void_p:
        """
        Gets the associated .net object pointer.

        Returns:
            ctypes.c_void_p: The .net object pointer.
        """
        return self._interop.get_interop_ptr__()
