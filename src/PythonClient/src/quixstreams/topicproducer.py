import ctypes
import traceback
from typing import Callable

from .helpers.nativedecorator import nativedecorator
from .native.Python.InteropHelpers.InteropUtils import InteropUtils
from .native.Python.QuixStreamsStreaming.ITopicProducer import ITopicProducer as tpi
from .streamproducer import StreamProducer


@nativedecorator
class TopicProducer(object):
    """
    Interface to operate with the streaming platform for publishing messages
    """

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
        Initializes a new instance of TopicProducer.

        NOTE: Do not initialize this class manually, use KafkaStreamingClient.get_topic_producer to create it.

        Args:
            net_pointer: The .net object representing a StreamingClient.
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
        Gets the handler for when the topic is disposed.

        Returns:
            Callable[[TopicProducer], None]: The event handler for topic disposal.
                The first parameter is the TopicProducer instance that got disposed.
        """
        return self._on_disposed

    @on_disposed.setter
    def on_disposed(self, value: Callable[['TopicProducer'], None]) -> None:
        """
        Sets the handler for when the topic is disposed.

        Args:
            value: The event handler for topic disposal.
                The first parameter is the TopicProducer instance that got disposed.
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
        Create a new stream and returns the related StreamProducer to operate it.

        Args:
            stream_id: Provide if you wish to overwrite the generated stream id. Useful if you wish
            to always stream a certain source into the same stream.

        Returns:
            StreamProducer: The created StreamProducer instance.
        """
        if stream_id is None:
            return StreamProducer(self, self._interop.CreateStream())
        return StreamProducer(self, self._interop.CreateStream2(stream_id))

    def get_stream(self, stream_id: str) -> StreamProducer:
        """
        Retrieves a stream that was previously created by this instance, if the stream is not closed.

        Args:
            stream_id: The id of the stream.

        Returns:
            StreamProducer: The retrieved StreamProducer instance or None if not found.
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
        Retrieves a stream that was previously created by this instance if the stream is not closed, otherwise creates a new stream.

        Args:
            stream_id: The id of the stream you want to get or create.
            on_stream_created: A callback function that takes a StreamProducer as a parameter.

        Returns:
            StreamProducer: The retrieved or created StreamProducer instance.
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
