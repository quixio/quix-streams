from typing import Callable

from .eventhook import EventHook
from .native.Python.InteropHelpers.InteropUtils import InteropUtils
from .streamwriter import StreamWriter
import ctypes

from .native.Python.QuixSdkStreaming.IOutputTopic import IOutputTopic as oti
from .helpers.nativedecorator import nativedecorator


@nativedecorator
class OutputTopic(object):
    """
        Interface to operate with the streaming platform for reading or writing
    """

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
            Initializes a new instance of OutputTopic.
            NOTE: Do not initialize this class manually, use StreamingClient.create_output to create it

            Parameters:

            net_object (.net object): The .net object representing a StreamingClient
        """

        self._cfuncrefs = []  # exists to hold onto the references created by the interop layer to avoid GC'ing them
        self._interop = oti(net_pointer)

        def _on_disposed_read_net_handler(sender):
            self.on_disposed.fire(self)
            InteropUtils.free_hptr(sender)

        def _on_disposed_first_sub():
            ref = self._interop.add_OnDisposed(_on_disposed_read_net_handler)
            self._cfuncrefs.append(ref)

        def _on_disposed_last_unsub():
            # TODO do unsign with previous handler
            self._interop.remove_OnDisposed(_on_disposed_read_net_handler)

        self.on_disposed = EventHook(_on_disposed_first_sub, _on_disposed_last_unsub, name="OutputTopic.on_disposed")
        """
        Raised when the resource finished disposing       
         Parameters:
            topic (OutputTopic): The OutputTopic which raises the event
        """

    def _finalizerfunc(self):
        self._cfuncrefs = None

    def create_stream(self, stream_id: str = None) -> StreamWriter:
        """
           Create new stream and returns the related stream writer to operate it.

           Parameters:

           stream_id (string): Optional, provide if you wish to overwrite the generated stream id. Useful if you wish
           to always stream a certain source into the same stream
       """
        if stream_id is None:
            return StreamWriter(self._interop.CreateStream())
        return StreamWriter(self._interop.CreateStream2(stream_id))

    def get_stream(self, stream_id: str) -> StreamWriter:
        """
           Retrieves a stream that was previously created by this instance, if the stream is not closed.

           Parameters:

           stream_id (string): The id of the stream
       """
        if stream_id is None:
            return None
        result_hptr = self._interop.GetStream(stream_id)
        if result_hptr is None:
            return None
        # TODO retrieving same stream constantly might result in weird behavior here
        return StreamWriter(result_hptr)

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
            def on_create_callback(streamwriter_hptr: ctypes.c_void_p):
                if type(streamwriter_hptr) is not ctypes.c_void_p:
                    streamwriter_hptr = ctypes.c_void_p(streamwriter_hptr)
                wrapped = StreamWriter(streamwriter_hptr)
                on_stream_created(wrapped)
            callback = on_create_callback

        return StreamWriter(self._interop.GetOrCreateStream(stream_id, callback)[0])

