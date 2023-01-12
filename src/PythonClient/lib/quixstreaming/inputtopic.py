from .eventhook import EventHook
from .streamreader import StreamReader
from . import __importnet
import Quix.Sdk.Streaming

class InputTopic(object):
    """
        Interface to operate with the streaming platform for reading or writing
    """

    def __init__(self, net_object):
        """
            Initializes a new instance of StreamingClient.
            NOTE: Do not initialize this class manually, use StreamingClient.create_input to create it

            Parameters:

            net_object (.net object): The .net object representing a InputTopic
        """
        self.__wrapped = net_object # the wrapped .net StreamingClient
        self.on_stream_received = EventHook(name="InputTopic.on_stream_received")
        """
        Raised when a new stream is received.       
         Parameters:
            reader (StreamReader): The StreamReader which raises new packages related to stream
        """

        def __on_stream_received_net_handler(sender, arg):
            self.on_stream_received.fire(StreamReader(Quix.Sdk.Streaming.IStreamReader(arg)))
        self.__wrapped.OnStreamReceived += __on_stream_received_net_handler

        self.on_streams_revoked = EventHook(name="InputTopic.on_streams_revoked")
        """
        Raised when the underlying source of data became unavailable for the streams affected by it       
         Parameters:
            readers (StreamReader[]): The StreamReaders revoked
        """

        def __on_streams_revoked_net_handler(sender, arg):
            revoked_arg = list(map(lambda x: StreamReader(Quix.Sdk.Streaming.IStreamReader(x)), arg))
            self.on_streams_revoked.fire(revoked_arg)
        self.__wrapped.OnStreamsRevoked += __on_streams_revoked_net_handler

        self.on_revoking = EventHook(name="InputTopic.on_revoking")
        """
        Raised when the underlying source of data will became unavailable, but depending on implementation commit might be possible at this point
        """

        def __on_revoking_net_handler(sender, arg):
            self.on_revoking.fire()
        self.__wrapped.OnRevoking += __on_revoking_net_handler

        self.on_committed = EventHook(name="InputTopic.on_committed")
        """
        Raised when underlying source committed data read up to this point
        """

        def __on_committed_net_handler(sender, arg):
            self.on_committed.fire()
        self.__wrapped.OnCommitted += __on_committed_net_handler

        self.on_committing = EventHook(name="InputTopic.on_committing")
        """
        Raised when underlying source is about to commit data read up to this point
        """

        def __on_committing_net_handler(sender, arg):
            self.on_committing.fire()
        self.__wrapped.OnCommitting += __on_committing_net_handler

    def start_reading(self):
        """
           Starts reading streams
           Use 'on_stream_received' event to read incoming streams
        """
        self.__wrapped.StartReading()

    def commit(self):
        """
           Commit packages read up until now
        """
        self.__wrapped.Commit()

    def dispose(self):
        """
            Disposes underlying .net instance.
        """
        self.__wrapped.Dispose()

