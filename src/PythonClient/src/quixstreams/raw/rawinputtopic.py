from typing import Callable

from quixstreams.native.Python.InteropHelpers.InteropUtils import InteropUtils

from .rawmessage import RawMessage

from ..native.Python.QuixSdkStreaming.Raw.RawInputTopic import RawInputTopic as riti

import ctypes
from ..helpers.nativedecorator import nativedecorator


@nativedecorator
class RawInputTopic(object):

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
        Initializes a new instance of StreamReader.
        NOTE: Do not initialize this class manually, use StreamingClient.on_stream_received to read streams

        :param net_pointer: Pointer to an instance of a .net RawInputTopic
        """

        if net_pointer is None:
            raise Exception("RawInputTopic is none")

        self._interop = riti(net_pointer)
        
        # define events and their ref holder
        self._on_message_read = None
        self._on_message_read_ref = None  # keeping reference to avoid GC

        self._on_error_occurred = None
        self._on_error_occurred_ref = None  # keeping reference to avoid GC

    def _finalizerfunc(self):
        self._on_message_read_dispose()

    # region on_message_read
    @property
    def on_message_read(self) -> Callable[['RawInputTopic', RawMessage], None]:
        """
        Gets the handler for when topic receives message. First parameter is the topic the message is received for, second is the RawMessage.
        """
        return self._on_message_read

    @on_message_read.setter
    def on_message_read(self, value: Callable[['RawInputTopic', RawMessage], None]) -> None:
        """
        Sets the handler for when topic receives message. First parameter is the topic the message is received for, second is the RawMessage.
        """
        self._on_message_read = value
        if self._on_message_read_ref is None:
            self._on_message_read_ref = self._interop.add_OnMessageRead(self._on_message_read_wrapper)

    def _on_message_read_wrapper(self, topic_hptr, message_hptr):
        # To avoid unnecessary overhead and complication, we're using the topic instance we already have
        self._on_message_read(self, RawMessage(message_hptr))
        InteropUtils.free_hptr(topic_hptr)

    def _on_message_read_dispose(self):
        if self._on_message_read_ref is not None:
            self._interop.remove_OnMessageRead(self._on_message_read_ref)
            self._on_message_read_ref = None
    # endregion on_message_read

    # region on_error_occurred
    @property
    def on_error_occurred(self) -> Callable[['RawInputTopic', BaseException], None]:
        """
        Gets the handler for when a stream experiences exception during the asynchronous write process. First parameter is the topic
         the error is received for, second is the exception.
        """
        return self._on_error_occurred

    @on_error_occurred.setter
    def on_error_occurred(self, value: Callable[['RawInputTopic', BaseException], None]) -> None:
        """
        Sets the handler for when a stream experiences exception during the asynchronous write process. First parameter is the topic
         the error is received for, second is the exception.
        """
        self._on_error_occurred = value
        if self._on_error_occurred_ref is None:
            self._on_error_occurred_ref = self._interop.add_OnErrorOccurred(self._on_error_occurred_wrapper)

    def _on_error_occurred_wrapper(self, topic_hptr, error_hptr):
        # To avoid unnecessary overhead and complication, we're using the topic instance we already have
        # TODO fix arg to be handled as exception
        #self._on_error_occurred(self, BaseException(arg.Message, type(arg)))
        InteropUtils.free_hptr(topic_hptr)
        InteropUtils.free_hptr(error_hptr)

    def _on_error_occurred_dispose(self):
        if self._on_error_occurred_ref is not None:
            self._interop.remove_OnErrorOccurred(self._on_error_occurred_ref)
            self._on_error_occurred_ref = None
    # endregion on_error_occurred

    def start_reading(self):
        """
        Starts reading from the stream
        """
        self._interop.StartReading()
