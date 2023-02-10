from typing import Callable

from .native.Python.QuixSdkStreaming.App import App as ai
import ctypes

from .native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource import CancellationTokenSource as ctsi
from .native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken import CancellationToken as cti

class CancellationTokenSource:
    """
        Signals to a System.Threading.CancellationToken that it should be canceled.
    """

    def __init__(self):
        """
            Creates a new instance of CancellationTokenSource\
        """
        self._interop = ctsi(ctsi.Constructor())

    def is_cancellation_requested(self):
        return self._interop.get_IsCancellationRequested()

    def cancel(self):
        self._interop.Cancel()

    @property
    def token(self):
        return CancellationToken(self._interop.get_Token())

    def get_net_pointer(self) -> ctypes.c_void_p:
        return self._interop.get_interop_ptr__()


class CancellationToken:

    def __init__(self, net_hpointer: ctypes.c_void_p):
        self._interop = cti(net_hpointer)

    def is_cancellation_requested(self):
        return self._interop.get_IsCancellationRequested()

    @staticmethod
    def get_none():
        return CancellationToken(cti.get_None())

    def get_net_pointer(self) -> ctypes.c_void_p:
        return self._interop.get_interop_ptr__()


class App():
    """
        Helper class to handle default streaming behaviors and handle automatic resource cleanup on shutdown
    """

    @staticmethod
    def run(cancellation_token: CancellationToken = None, before_shutdown: Callable[[], None] = None):
        """
            Helper method to handle default streaming behaviors and handle automatic resource cleanup on shutdown
            It also ensures input topics defined at the time of invocation are opened for read.

            :param cancellation_token: An optional cancellation token to abort the application run with
            :param before_shutdown: An optional function to call before shutting down
        """

        def wrapper():
            if before_shutdown is not None:
                before_shutdown()

        try:
            if cancellation_token is not None:
                ai.Run(cancellationToken=cancellation_token.get_net_pointer(), beforeShutdown=wrapper)
            else:
                ai.Run(beforeShutdown=wrapper)
        except KeyboardInterrupt:
            pass
