import ctypes
import traceback
from typing import Callable, List

from .helpers.nativedecorator import nativedecorator
from .native.Python.InteropHelpers.InteropUtils import InteropUtils
from .native.Python.QuixStreamsStreaming.TopicConsumer import TopicConsumer as tci
from .streamconsumer import StreamConsumer


@nativedecorator
class TopicConsumer(object):
    """
    Interface to operate with the streaming platform for consuming messages
    """

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
        Initializes a new instance of TopicConsumer.

        NOTE: Do not initialize this class manually, use KafkaStreamingClient.get_topic_consumer to create it.

        Args:
            net_pointer: The .net pointer to TopicConsumer instance.
        """
        if net_pointer is None:
            raise Exception("TopicConsumer is None")

        self._interop = tci(net_pointer)

        self._active_streams = []  # To clean up when closing topic else could end up with references

        # define events and their ref holder
        self._on_stream_received = None
        self._on_stream_received_ref = None  # keeping reference to avoid GC

        self._on_streams_revoked = None
        self._on_streams_revoked_ref = None  # keeping reference to avoid GC

        self._on_revoking = None
        self._on_revoking_ref = None  # keeping reference to avoid GC

        self._on_committed = None
        self._on_committed_ref = None  # keeping reference to avoid GC

        self._on_committing = None
        self._on_committing_ref = None  # keeping reference to avoid GC

    def _finalizerfunc(self):
        self._on_stream_received_dispose()
        self._on_streams_revoked_dispose()
        self._on_revoking_dispose()
        self._on_committing_dispose()
        self._on_committed_dispose()
        self._active_streams = None

    # region on_stream_received
    @property
    def on_stream_received(self) -> Callable[['StreamConsumer'], None]:
        """
        Gets the event handler for when a stream is received for the topic.

        Returns:
            Callable[[StreamConsumer], None]: The event handler for when a stream is received for the topic.
                The first parameter is the StreamConsumer instance that was received.
        """
        return self._on_stream_received

    @on_stream_received.setter
    def on_stream_received(self, value: Callable[['StreamConsumer'], None]) -> None:
        """
        Sets the event handler for when a stream is received for the topic.

        Args:
            value: The new event handler for when a stream is received for the topic.
                The first parameter is the StreamConsumer instance that was received.
        """
        self._on_stream_received = value
        if self._on_stream_received_ref is None:
            self._on_stream_received_ref = self._interop.add_OnStreamReceived(self._on_stream_received_wrapper)

    def _on_stream_received_wrapper(self, topic_hptr, stream_hptr):
        # To avoid unnecessary overhead and complication, we're using the topic instance we already have
        try:
            def remove_active_stream(stream):
                if self._active_streams is not None:
                    self._active_streams.remove(stream)

            stream = StreamConsumer(stream_hptr, self, remove_active_stream)
            self._active_streams.append(stream)
            self._on_stream_received(stream)
            InteropUtils.free_hptr(topic_hptr)
        except:
            traceback.print_exc()

    def _on_stream_received_dispose(self):
        if self._on_stream_received_ref is not None:
            self._interop.remove_OnStreamReceived(self._on_stream_received_ref)
            self._on_stream_received_ref = None

    # endregion on_stream_received

    # region on_streams_revoked
    @property
    def on_streams_revoked(self) -> Callable[['TopicConsumer', List['StreamConsumer']], None]:
        """
        Gets the event handler for when streams are revoked for the topic.

        Returns:
            Callable[[TopicConsumer, List[StreamConsumer]], None]: The event handler for when streams are revoked for the topic.
                The first parameter is the TopicConsumer instance for which the streams were revoked, and the second parameter is a list of StreamConsumer instances that were revoked.
        """
        return self._on_streams_revoked

    @on_streams_revoked.setter
    def on_streams_revoked(self, value: Callable[['TopicConsumer', List['StreamConsumer']], None]) -> None:
        """
        Sets the event handler for when streams are revoked for the topic.

        Args:
            value: The new event handler for when streams are revoked for the topic.
                The first parameter is the TopicConsumer instance for which the streams were revoked, and the second parameter is a list of StreamConsumer instances that were revoked.
        """

        self._on_streams_revoked = value
        if self._on_streams_revoked_ref is None:
            self._on_streams_revoked_ref = self._interop.add_OnStreamsRevoked(self._on_streams_revoked_wrapper)

    def _on_streams_revoked_wrapper(self, topic_hptr, streams_uptr):
        # To avoid unnecessary overhead and complication, we're using the instances we already have
        # TODO
        # revoked_arg = list(map(lambda x: StreamConsumer(QuixStreams.Streaming.IStreamConsumer(x)), arg))
        # self.on_streams_revoked.fire(revoked_arg)
        streams = []
        try:
            self._on_streams_revoked(self, streams)
            InteropUtils.free_hptr(topic_hptr)
        except:
            traceback.print_exc()

    def _on_streams_revoked_dispose(self):
        if self._on_streams_revoked_ref is not None:
            self._interop.remove_OnStreamsRevoked(self._on_streams_revoked_ref)
            self._on_streams_revoked_ref = None

    # endregion on_streams_revoked

    # region on_revoking
    @property
    def on_revoking(self) -> Callable[['TopicConsumer'], None]:
        """
        Gets the event handler for when the topic is being revoked.

        Returns:
            Callable[[TopicConsumer], None]: The event handler for when the topic is being revoked.
                The first parameter is the TopicConsumer instance for which the revocation is happening.
        """
        return self._on_revoking

    @on_revoking.setter
    def on_revoking(self, value: Callable[['TopicConsumer'], None]) -> None:
        """
        Sets the event handler for when the topic is being revoked.

        Args:
            value: The new event handler for when the topic is being revoked.
                The first parameter is the TopicConsumer instance for which the revocation is happening.
        """
        self._on_revoking = value
        if self._on_revoking_ref is None:
            self._on_revoking_ref = self._interop.add_OnRevoking(self._on_revoking_wrapper)

    def _on_revoking_wrapper(self, topic_hptr, args_hptr):
        # To avoid unnecessary overhead and complication, we're using the topic instance we already have
        try:
            self._on_revoking(self)
            InteropUtils.free_hptr(topic_hptr)
            InteropUtils.free_hptr(args_hptr)
        except:
            traceback.print_exc()

    def _on_revoking_dispose(self):
        if self._on_revoking_ref is not None:
            self._interop.remove_OnRevoking(self._on_revoking_ref)
            self._on_revoking_ref = None

    # endregion on_revoking

    # region on_committed
    @property
    def on_committed(self) -> Callable[['TopicConsumer'], None]:
        """
        Gets the event handler for when the topic finishes committing consumed data up to this point.

        Returns:
            Callable[[TopicConsumer], None]: The event handler for when the topic finishes committing consumed data up to this point.
                The first parameter is the TopicConsumer instance for which the commit happened.
        """
        return self._on_committed

    @on_committed.setter
    def on_committed(self, value: Callable[['TopicConsumer'], None]) -> None:
        """
        Sets the event handler for when the topic finishes committing consumed data up to this point.

        Args:
            value: The new event handler for when the topic finishes committing consumed data up to this point.
                The first parameter is the TopicConsumer instance for which the commit happened.
        """
        self._on_committed = value
        if self._on_committed_ref is None:
            self._on_committed_ref = self._interop.add_OnCommitted(self._on_committed_wrapper)

    def _on_committed_wrapper(self, topic_hptr, args_hptr):
        # To avoid unnecessary overhead and complication, we're using the topic instance we already have
        try:
            self._on_committed(self)
            InteropUtils.free_hptr(topic_hptr)
            InteropUtils.free_hptr(args_hptr)
        except:
            traceback.print_exc()

    def _on_committed_dispose(self):
        if self._on_committed_ref is not None:
            self._interop.remove_OnCommitted(self._on_committed_ref)
            self._on_committed_ref = None

    # endregion on_committed

    # region on_committing
    @property
    def on_committing(self) -> Callable[['TopicConsumer'], None]:
        """
        Gets the event handler for when the topic begins committing consumed data up to this point.

        Returns:
            Callable[[TopicConsumer], None]: The event handler for when the topic begins committing consumed data up to this point.
                The first parameter is the TopicConsumer instance for which the commit is happening.
        """
        return self._on_committing

    @on_committing.setter
    def on_committing(self, value: Callable[['TopicConsumer'], None]) -> None:
        """
        Sets the event handler for when the topic begins committing consumed data up to this point.

        Args:
            value: The new event handler for when the topic begins committing consumed data up to this point.
                The first parameter is the TopicConsumer instance for which the commit is happening.
        """
        self._on_committing = value
        if self._on_committing_ref is None:
            self._on_committing_ref = self._interop.add_OnCommitting(self._on_committing_wrapper)

    def _on_committing_wrapper(self, topic_hptr, args_hptr):
        # To avoid unnecessary overhead and complication, we're using the topic instance we already have
        try:
            self._on_committing(self)
            InteropUtils.free_hptr(topic_hptr)
            InteropUtils.free_hptr(args_hptr)
        except:
            traceback.print_exc()

    def _on_committing_dispose(self):
        if self._on_committing_ref is not None:
            self._interop.remove_OnCommitting(self._on_committing_ref)
            self._on_committing_ref = None

    # endregion on_committing

    def subscribe(self):
        """
        Subscribes to streams in the topic.
        Use 'on_stream_received' event to consume incoming streams.
        """
        self._interop.Subscribe()

    def commit(self):
        """
        Commit packages consumed up until now
        """
        self._interop.Commit()

    def get_net_pointer(self) -> ctypes.c_void_p:
        """
        Retrieves the .net pointer to TopicConsumer instance.

        Returns:
            ctypes.c_void_p: The .net pointer to TopicConsumer instance.
        """
        return self._interop.get_interop_ptr__()
