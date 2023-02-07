from .eventhook import EventHook
from .native.Python.InteropHelpers.InteropUtils import InteropUtils
from .streamreader import StreamReader

import ctypes
from .native.Python.QuixSdkStreaming.InputTopic import InputTopic as iti
from .helpers.nativedecorator import nativedecorator


@nativedecorator
class InputTopic(object):
    """
        Interface to operate with the streaming platform for reading or writing
    """

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
            Initializes a new instance of StreamingClient.
            NOTE: Do not initialize this class manually, use StreamingClient.create_input to create it

            Parameters:

            net_pointer (.net pointer): The .net pointer to InputTopic instance
        """
        self._cfuncrefs = []  # exists to hold onto the references created by the interop layer to avoid GC'ing them
        self._active_streams = []  # To clean up when closing topic else could end up with references
        self._interop = iti(net_pointer)


        def _on_stream_received_read_net_handler(sender, arg):
            stream = StreamReader(arg)
            self._active_streams.append(stream)
            stream.on_stream_closed += lambda x: self._active_streams.remove(stream)
            self.on_stream_received.fire(stream)
            InteropUtils.free_hptr(sender)  # another pointer is assigned to the same object as current, we don't need it

        def _on_stream_received_first_sub():
            ref = self._interop.add_OnStreamReceived(_on_stream_received_read_net_handler)
            self._cfuncrefs.append(ref)

        def _on_stream_received_last_unsub():
            # TODO do unsign with previous handler
            self._interop.remove_OnStreamReceived(_on_stream_received_read_net_handler)

        self.on_stream_received = EventHook(_on_stream_received_first_sub, _on_stream_received_last_unsub, name="InputTopic.on_stream_received")
        """
        Raised when a new stream is received.       
         Parameters:
            reader (StreamReader): The StreamReader which raises new packages related to stream
        """

        def _on_streams_revoked_read_net_handler(sender, arg):
            # TODO
            # revoked_arg = list(map(lambda x: StreamReader(Quix.Sdk.Streaming.IStreamReader(x)), arg))
            # self.on_streams_revoked.fire(revoked_arg)
            InteropUtils.free_hptr(sender)  # another pointer is assigned to the same object as current, we don't need it

        def _on_streams_revoked_first_sub():
            ref = self._interop.add_OnStreamsRevoked(_on_streams_revoked_read_net_handler)
            self._cfuncrefs.append(ref)

        def _on_streams_revoked_last_unsub():
            # TODO do unsign with previous handler
            self._interop.remove_OnStreamsRevoked(_on_streams_revoked_read_net_handler)

        self.on_streams_revoked = EventHook(_on_streams_revoked_first_sub, _on_streams_revoked_last_unsub, name="InputTopic.on_streams_revoked")
        """
        Raised when the underlying source of data became unavailable for the streams affected by it       
         Parameters:
            readers (StreamReader[]): The StreamReaders revoked
        """

        def _on_revoking_read_net_handler(sender, arg):
            #TODO
            #self.on_revoking.fire()
            InteropUtils.free_hptr(sender)  # another pointer is assigned to the same object as current, we don't need it

        def _on_revoking_first_sub():
            ref = self._interop.add_OnRevoking(_on_revoking_read_net_handler)
            self._cfuncrefs.append(ref)

        def _on_revoking_last_unsub():
            # TODO do unsign with previous handler
            self._interop.remove_OnRevoking(_on_revoking_read_net_handler)

        self.on_revoking = EventHook(_on_revoking_first_sub, _on_revoking_last_unsub, name="InputTopic.on_revoking")
        """
        Raised when the underlying source of data will became unavailable, but depending on implementation commit might be possible at this point
        """

        def _on_committed_read_net_handler(sender, arg):
            # TODO
            #self.on_committed.fire()
            InteropUtils.free_hptr(sender)  # another pointer is assigned to the same object as current, we don't need it

        def _on_committed_first_sub():
            ref = self._interop.add_OnCommitted(_on_committed_read_net_handler)
            self._cfuncrefs.append(ref)

        def _on_committed_last_unsub():
            # TODO do unsign with previous handler
            self._interop.remove_OnCommitted(_on_committed_read_net_handler)

        self.on_committed = EventHook(_on_committed_first_sub, _on_committed_last_unsub, name="InputTopic.on_committed")
        """
        Raised when underlying source committed data read up to this point
        """

        def _on_committing_read_net_handler(sender, arg):
            # TODO
            #self.on_committing.fire()
            InteropUtils.free_hptr(sender)  # another pointer is assigned to the same object as current, we don't need it

        def _on_committing_first_sub():
            ref = self._interop.add_OnCommitting(_on_committing_read_net_handler)
            self._cfuncrefs.append(ref)

        def _on_committing_last_unsub():
            # TODO do unsign with previous handler
            self._interop.remove_OnCommitting(_on_committing_read_net_handler)

        self.on_committing = EventHook(_on_committing_first_sub, _on_committing_last_unsub, name="InputTopic.on_committing")
        """
        Raised when underlying source is about to commit data read up to this point
        """

    def _finalizerfunc(self):
        self._cfuncrefs = None

    def start_reading(self):
        """
           Starts reading streams
           Use 'on_stream_received' event to read incoming streams
        """
        self._interop.StartReading()

    def commit(self):
        """
           Commit packages read up until now
        """
        self._interop.Commit()

    def get_net_pointer(self) -> ctypes.c_void_p:
        return self._interop.get_interop_ptr__()
