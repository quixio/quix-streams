from ..eventhook import EventHook

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
        self._cfuncrefs = []  # exists to hold onto the references created by the interop layer to avoid GC'ing them


        def _on_message_read_handler(arg_hptr):
            # the type passed in will be an int, but RawMessage only takes pointer or forms of byte[]
            raw_message = RawMessage(ctypes.c_void_p(arg_hptr))
            self.on_message_read.fire(raw_message)

        def _on_first_message_sub():
            ref = self._interop.add_OnMessageRead(_on_message_read_handler)
            self._cfuncrefs.append(ref)

        def _on_last_message_unsub():
            # TODO fix unsub
            self._interop.remove_OnMessageRead(_on_message_read_handler)

        self.on_message_read = EventHook(_on_first_message_sub, _on_last_message_unsub, name="RawInputTopic.on_message_read")

        def _on_error_occured_handler(sender_hptr, exception_hptr):
            # TODO
            raise Exception("NOT IMPLEMENTED")
            self.on_error_occurred.fire()

        def _on_error_first_sub():
            ref = self._interop.add_OnErrorOccurred(_on_error_occured_handler)
            self._cfuncrefs.append(ref)

        def _on_error_last_unsub():
            # TODO fix unsub
            self._interop.remove_OnMessageRead(_on_error_occured_handler)

        self.on_error_occurred = EventHook(_on_error_first_sub, _on_error_last_unsub, name="RawInputTopic.on_error_occurred")

    def _finalizerfunc(self):
        self._cfuncrefs = None

    def start_reading(self):
        """
        Starts reading from the stream
        """
        self._interop.StartReading()
