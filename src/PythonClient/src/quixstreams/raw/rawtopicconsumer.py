import ctypes
import traceback
from typing import Callable

from ..native.Python.InteropHelpers.InteropUtils import InteropUtils
from .rawmessage import RawMessage
from ..helpers.nativedecorator import nativedecorator
from ..native.Python.QuixStreamsStreaming.Raw.RawTopicConsumer import RawTopicConsumer as rtpi


@nativedecorator
class RawTopicConsumer(object):
    """
    Topic class to consume incoming raw messages (capable to consuming non-quixstreams messages).
    """

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
       Initializes a new instance of RawTopicConsumer.

       Note:
           Do not initialize this class manually, use KafkaStreamingClient.get_raw_topic_consumer.

       Args:
           net_pointer: Pointer to an instance of a .net RawTopicConsumer.
       """

        if net_pointer is None:
            raise Exception("RawTopicConsumer is none")

        self._interop = rtpi(net_pointer)

        # define events and their ref holder
        self._on_message_received = None
        self._on_message_received_ref = None  # keeping reference to avoid GC

        self._on_error_occurred = None
        self._on_error_occurred_ref = None  # keeping reference to avoid GC

    def _finalizerfunc(self):
        self._on_message_received_dispose()

    # region on_message_received
    @property
    def on_message_received(self) -> Callable[['RawTopicConsumer', RawMessage], None]:
        """
        Gets the handler for when a topic receives a message.

        Returns:
            Callable[[RawTopicConsumer, RawMessage], None]: The event handler for when a topic receives a message.
                The first parameter is the RawTopicConsumer instance for which the message is received, and the second is the RawMessage.
        """

        return self._on_message_received

    @on_message_received.setter
    def on_message_received(self, value: Callable[['RawTopicConsumer', RawMessage], None]) -> None:
        """
        Sets the handler for when a topic receives a message.

        Args:
            value: The new event handler for when a topic receives a message.
                The first parameter is the RawTopicConsumer instance for which the message is received, and the second is the RawMessage.
        """
        self._on_message_received = value
        if self._on_message_received_ref is None:
            self._on_message_received_ref = self._interop.add_OnMessageReceived(self._on_message_received_wrapper)

    def _on_message_received_wrapper(self, topic_hptr, message_hptr):
        # To avoid unnecessary overhead and complication, we're using the topic instance we already have
        try:
            self._on_message_received(self, RawMessage(message_hptr))
            InteropUtils.free_hptr(topic_hptr)
        except:
            traceback.print_exc()

    def _on_message_received_dispose(self):
        if self._on_message_received_ref is not None:
            self._interop.remove_OnMessageReceived(self._on_message_received_ref)
            self._on_message_received_ref = None

    # endregion on_message_received

    # region on_error_occurred
    @property
    def on_error_occurred(self) -> Callable[['RawTopicConsumer', BaseException], None]:
        """
        Gets the handler for when a stream experiences an exception during the asynchronous write process.

        Returns:
            Callable[[RawTopicConsumer, BaseException], None]: The event handler for when a stream experiences an exception during the asynchronous write process.
                The first parameter is the RawTopicConsumer instance for which the error is received, and the second is the exception.
        """
        return self._on_error_occurred

    @on_error_occurred.setter
    def on_error_occurred(self, value: Callable[['RawTopicConsumer', BaseException], None]) -> None:
        """
        Sets the handler for when a stream experiences an exception during the asynchronous write process.

        Args:
            value: The new handler for when a stream experiences an exception during the asynchronous write process.
                The first parameter is the RawTopicConsumer instance for which the error is received, and the second is the exception.
        """
        self._on_error_occurred = value
        if self._on_error_occurred_ref is None:
            self._on_error_occurred_ref = self._interop.add_OnErrorOccurred(self._on_error_occurred_wrapper)

    def _on_error_occurred_wrapper(self, topic_hptr, error_hptr):
        # To avoid unnecessary overhead and complication, we're using the topic instance we already have
        try:
            # TODO fix arg to be handled as exception
            # self._on_error_occurred(self, BaseException(arg.Message, type(arg)))
            InteropUtils.free_hptr(topic_hptr)
            InteropUtils.free_hptr(error_hptr)
        except:
            traceback.print_exc()

    def _on_error_occurred_dispose(self):
        if self._on_error_occurred_ref is not None:
            self._interop.remove_OnErrorOccurred(self._on_error_occurred_ref)
            self._on_error_occurred_ref = None

    # endregion on_error_occurred

    def subscribe(self):
        """
        Starts subscribing to the streams.
        """
        self._interop.Subscribe()
