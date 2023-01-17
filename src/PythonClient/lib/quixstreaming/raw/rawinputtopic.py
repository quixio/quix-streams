from quixstreaming.eventhook import EventHook

import Quix.Sdk.Streaming.Raw
from .rawmessage import RawMessage

class RawInputTopic(object):
    def __init__(self, net_object: Quix.Sdk.Streaming.Raw.RawInputTopic):
        self.__wrapped = net_object

        #
        #   define the on_message_read event handler
        #
        def __on_message_read_handler(arg):
            self.on_message_read.fire(RawMessage(arg))
        def __on_first_sub():
            self.__wrapped.OnMessageRead += __on_message_read_handler
        def __on_last_unsub():
            self.__wrapped.OnMessageRead -= __on_message_read_handler

        self.on_message_read = EventHook(__on_first_sub, __on_last_unsub, name="RawInputTopic.on_message_read")


        #
        #   define the on_error_occured event handler
        #
        def __on_error_occured_handler(arg):
            self.on_message_read.fire(RawMessage(arg))
        def __on_error_first_sub():
            self.__wrapped.ErrorOccured += __on_error_occured_handler
        def __on_error_last_unsub():
            self.__wrapped.ErrorOccured -= __on_error_occured_handler

        self.on_error_occured = EventHook(__on_error_first_sub, __on_error_last_unsub, name="RawInputTopic.on_error_occured")




    def start_reading(self):
        """
        Starts reading from the stream
        """
        self.__wrapped.StartReading()
