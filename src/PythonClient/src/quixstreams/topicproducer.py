import ctypes
import traceback
from typing import Callable

from .helpers.nativedecorator import nativedecorator
from .native.Python.InteropHelpers.InteropUtils import InteropUtils
from .native.Python.QuixSdkStreaming.ITopicProducer import ITopicProducer as tpi
from .streamproducer import StreamProducer


@nativedecorator
class TopicProducer(object):
    """
        Interface to operate with the streaming platform for reading or writing
    """

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
            Initializes a new instance of TopicProducer.
            NOTE: Do not initialize this class manually, use StreamingClient.create_output to create it

            Parameters:

            net_object (.net object): The .net object representing a StreamingClient
        """

        self._interop = tpi(net_pointer)

        # define events and their ref holder
        self._on_disposed = None
        self._on_disposed_ref = None  # keeping reference to avoid GC

    def _finalizerfunc(self):
        self._on_disposed_dispose()

    # region on_disposed
    @property
    def on_disposed(self) -> Callable[['TopicProducer'], None]:
        """
        Gets the handler for when the topic is disposed. First parameter is the topic which got disposed.
        """
        return self._on_disposed

    @on_disposed.setter
    def on_disposed(self, value: Callable[['TopicProducer'], None]) -> None:
        """
        Sets the handler for when the topic is disposed. First parameter is the topic which got disposed.
        """
        self._on_disposed = value
        if self._on_disposed_ref is None:
            self._on_disposed_ref = self._interop.add_OnDisposed(self._on_disposed_wrapper)

    def _on_disposed_wrapper(self, stream_hptr, arg_hptr):
        # To avoid unnecessary overhead and complication, we're using the stream instance we already have
        try:
            self._on_disposed(self)
            InteropUtils.free_hptr(stream_hptr)
            InteropUtils.arg_hptr(arg_hptr)
        except:
            traceback.print_exc()

    def _on_disposed_dispose(self):
        if self._on_disposed_ref is not None:
            self._interop.remove_OnDisposed(self._on_disposed_ref)
            self._on_disposed_ref = None

    # endregion on_disposed

    def create_stream(self, stream_id: str = None) -> StreamProducer:
        """
           Create new stream and returns the related stream writer to operate it.

           Parameters:

           stream_id (string): Optional, provide if you wish to overwrite the generated stream id. Useful if you wish
           to always stream a certain source into the same stream
       """
        if stream_id is None:
            return StreamProducer(self, self._interop.CreateStream())
        return StreamProducer(self, self._interop.CreateStream2(stream_id))

    def get_stream(self, stream_id: str) -> StreamProducer:
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
        return StreamProducer(self, result_hptr)

    def get_or_create_stream(self, stream_id: str, on_stream_created: Callable[[StreamProducer], None] = None) -> StreamProducer:
        """
           Retrieves a stream that was previously created by this instance, if the stream is not closed, otherwise creates a new stream.

           Parameters:

           stream_id (string): The Id of the stream you want to get or create
           on_stream_created (Callable[[StreamProducer], None]): A void callback taking StreamProducer
       """

        if stream_id is None:
            return None

        callback = None
        if on_stream_created is not None:
            def on_create_callback(streamproducer_hptr: ctypes.c_void_p):
                if type(streamproducer_hptr) is not ctypes.c_void_p:
                    streamproducer_hptr = ctypes.c_void_p(streamproducer_hptr)
                wrapped = StreamProducer(self, streamproducer_hptr)
                on_stream_created(wrapped)

            callback = on_create_callback

        return StreamProducer(self, self._interop.GetOrCreateStream(stream_id, callback)[0])
