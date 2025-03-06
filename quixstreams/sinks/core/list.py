from collections import UserList
from typing import Any

from quixstreams.models import HeadersTuples
from quixstreams.sinks.base import BaseSink


class ListSink(BaseSink, UserList):
    """
    A sink that accumulates data into a list for interactive debugging/inspection.

    It behaves just like a list. Messages are appended as they are handled by it.

    You can optionally include the message metadata as well.

    Intended for debugging with Application.run(timeout=N)

    Example:
    ```
    from quixstreams import Application
    from quixstreams.sinks.core.list import ListSink

    app = Application(broker_address="localhost:9092")
    topic = app.topic("some-topic")
    list_sink = ListSink()  # sink will be a list-like object
    sdf = app.dataframe(topic=topic).sink(list_sink)
    app.run(timeout=10)  # collect data for 10 seconds

    # after running 10s
    print(list_sink)    # [1, 2, 3]
    list_sink[0]        # 1
    ```

    Metadata Behavior:
        When initialized with metadata=True, each record will be
        enriched with the following metadata fields as a prefix
        to the value dictionary:
        - _key: The message key
        - _timestamp: Message timestamp
        - _headers: String representation of message headers
        - _topic: Source topic name
        - _partition: Source partition number
        - _offset: Message offset in partition
        When metadata=False (default), only the message value is stored.
    """

    def __init__(self, *args, metadata: bool = False):
        # parsing args this way ensures UserList operates correctly with certain
        # functions (like slicing) since it uses positional args internally while
        # simultaneously obscuring the positional arg from the user
        UserList.__init__(self, initlist=args[0] if args else None)
        BaseSink.__init__(self)
        self._metadata = metadata

    def setup(self):
        return

    def add(
        self,
        value: Any,
        key: Any,
        timestamp: int,
        headers: HeadersTuples,
        topic: str,
        partition: int,
        offset: int,
    ):
        if not isinstance(value, dict):
            value = {"value": value}

        if self._metadata:
            value = {
                "_key": key,
                "_timestamp": timestamp,
                "_headers": headers,
                "_topic": topic,
                "_partition": partition,
                "_offset": offset,
                **value,
            }

        self.append(value)

    def flush(self, *args, **kwargs):
        return
