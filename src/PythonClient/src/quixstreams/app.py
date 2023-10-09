import ctypes
import logging
import traceback
import signal
from typing import Callable

from .native.Python.QuixStreamsStreaming.App import App as ai
from .native.Python.SystemPrivateCoreLib.System.Threading.CancellationToken import CancellationToken as cti
from .native.Python.SystemPrivateCoreLib.System.Threading.CancellationTokenSource import CancellationTokenSource as ctsi
from .states.appstatemanager import AppStateManager
from .state.istatestorage import IStateStorage


class CancellationTokenSource:
    """
    Represents a token source that can signal a cancellation System.Threading.CancellationToken
    """

    def __init__(self):
        """
        Initializes a new instance of the CancellationTokenSource class.
        """
        self._interop = ctsi(ctsi.Constructor())

    def is_cancellation_requested(self):
        """
        Checks if a cancellation has been requested.

        Returns:
            bool: True if the cancellation has been requested, False otherwise.
        """
        return self._interop.get_IsCancellationRequested()

    def cancel(self) -> None:
        """
        Signals a cancellation to the CancellationToken.
        """
        self._interop.Cancel()

    @property
    def token(self) -> 'CancellationToken':
        """
        Gets the associated CancellationToken.

        Returns:
            CancellationToken: The CancellationToken associated with this CancellationTokenSource.
        """
        return CancellationToken(self._interop.get_Token())

    def get_net_pointer(self) -> ctypes.c_void_p:
        """
        Gets the interop pointer of the CancellationTokenSource object.

        Returns:
            ctypes.c_void_p: The interop pointer of the CancellationTokenSource object.
        """
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


class App:
    """
    Provides utilities to handle default streaming behaviors and automatic resource cleanup on shutdown.
    """

    @staticmethod
    def run(cancellation_token: CancellationToken = None,
            before_shutdown: Callable[[], None] = None,
            subscribe: bool = True):
        """
        Runs the application, managing streaming behaviors and automatic resource cleanup on shutdown.

        Args:
            cancellation_token: An optional CancellationToken to abort the application run with.
            before_shutdown: An optional function to call before shutting down the application.
            subscribe: Whether the consumer defined should be automatically subscribed to start receiving messages
        """

        def wrapper():
            if before_shutdown is not None:
                try:
                    before_shutdown()
                except:
                    traceback.print_exc()

        # do nothing when the keyboard interrupt happens.
        # without this, the callback function invoked from interop would check for exceptions before anything else
        # resulting in a KeyboardInterrupt before any try/catch can be done. Given we're already handling this inside
        # the interop, there is no need to throw an exception that is impossible to handle anyway
        def keyboard_interrupt_handler(signal, frame):
            pass
        try:
            signal.signal(signal.SIGINT, keyboard_interrupt_handler)
        except ValueError as ex:
            # If this exception happens, it means the signal handling is running on non-main thread.
            # The end result is that keyboard or shutdown interruption will not work here or in C# interop.
            # While that is not optimal, it is still better to let the application at least function
            # and log the exception + warning than completely block it from functioning.
            traceback.print_exc()
            logging.log(logging.WARNING, "Shutdown may not work as expected. See error.")


        try:
            if cancellation_token is not None:
                ai.Run(cancellationToken=cancellation_token.get_net_pointer(), beforeShutdown=wrapper, subscribe=subscribe)
            else:
                ai.Run(beforeShutdown=wrapper, subscribe=subscribe)
        except KeyboardInterrupt:
            pass

    @staticmethod
    def get_state_manager() -> AppStateManager:
        """
        Retrieves the state manager for the application

        Returns:
            AppStateManager: the app's state manager
        """
        return AppStateManager(ai.GetStateManager())

    @staticmethod
    def set_state_storage(storage: IStateStorage) -> None:
        """
        Sets the state storage for the app

        Args:
            storage: The state storage to use for app's state manager
        """
        return ai.SetStateStorage(storage.get_net_pointer())
