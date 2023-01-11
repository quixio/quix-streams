from typing import Callable

from .eventhook import EventHook
from .streamwriter import StreamWriter
from . import __importnet
import Quix.Sdk.Streaming
import System


class OutputTopic(object):
    """
        Interface to operate with the streaming platform for reading or writing
    """

    def __init__(self, net_object):
        """
            Initializes a new instance of OutputTopic.
            NOTE: Do not initialize this class manually, use StreamingClient.create_output to create it

            Parameters:

            net_object (.net object): The .net object representing a StreamingClient
        """
        self.__wrapped = net_object # the wrapped .net OutputTopic

        self.on_disposed = EventHook(name="OutputTopic.on_disposed")
        """
        Raised when the resource finished disposing       
         Parameters:
            topic (OutputTopic): The OutputTopic which raises the event
        """

        def __on_topic_disposing_net_handler(sender, arg):
            self.on_disposed.fire(self)
        self.__wrapped.OnDisposed += __on_topic_disposing_net_handler

    def dispose(self):
        """
            Disposes underlying .net instance.
        """
        self.__wrapped.Dispose()

    def create_stream(self, stream_id: str = None) -> StreamWriter:
        """
           Create new stream and returns the related stream writer to operate it.

           Parameters:

           stream_id (string): Optional, provide if you wish to overwrite the generated stream id. Useful if you wish
           to always stream a certain source into the same stream
       """
        if stream_id is None:
            return StreamWriter(Quix.Sdk.Streaming.IStreamWriter(self.__wrapped.CreateStream()))
        return StreamWriter(Quix.Sdk.Streaming.IStreamWriter(self.__wrapped.CreateStream(stream_id)))

    def get_stream(self, stream_id: str) -> StreamWriter:
        """
           Retrieves a stream that was previously created by this instance, if the stream is not closed.

           Parameters:

           stream_id (string): The id of the stream
       """
        if stream_id is None:
            return None
        result = self.__wrapped.GetStream(stream_id)
        if result is None:
            return None
        return StreamWriter(Quix.Sdk.Streaming.IStreamWriter(result))

    def get_or_create_stream(self, stream_id: str, on_stream_created: Callable[[StreamWriter], None] = None) -> StreamWriter:
        """
           Retrieves a stream that was previously created by this instance, if the stream is not closed, otherwise creates a new stream.

           Parameters:

           stream_id (string): The Id of the stream you want to get or create
           on_stream_created (Callable[[StreamWriter], None]): A void callback taking StreamWriter
       """

        if stream_id is None:
            return None

        callback = None
        if on_stream_created is not None:
            def on_create_callback(sw: Quix.Sdk.Streaming.IStreamWriter):
                wrapped = StreamWriter(sw)
                on_stream_created(wrapped)
            callback = System.Action[Quix.Sdk.Streaming.IStreamWriter](on_create_callback)

        return StreamWriter(Quix.Sdk.Streaming.IStreamWriter(self.__wrapped.GetOrCreateStream(stream_id, callback)))

