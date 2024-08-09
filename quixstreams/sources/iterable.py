import logging

from typing import Iterable, Optional, Tuple

from quixstreams.models.messages import KafkaMessage

from .base import PollingSource

logger = logging.getLogger(__name__)


__all__ = (
    "ValueIterableSource",
    "KeyValueIterableSource",
)


class ValueIterableSource(PollingSource):
    """
    Source implementation that iterate over values with a fix key.

    Example Snippet:

    ```python
    from quixstreams import Application
    from quixstreams.sources import ValueIterableSource

    app = Application(broker_address='localhost:9092', consumer_group='group')
    source = ValueIterableSource(name="my_source", values=iter(range(10)))

    sdf = app.dataframe(source=source)
    sdf.print()

    app.run(sdf)
    ```
    """

    def __init__(
        self,
        name: str,
        values: Iterable[object],
        key: Optional[object] = None,
        shutdown_timeout: int = 10,
    ) -> None:
        """
        :param name: The source unique name. Used to generate the default topic
        :param values: The values iterable.
        :param key: The fixed key.
        :param shutdown_timeout: Time in second the application waits for the source to gracefully shutdown
        """
        super().__init__(name, shutdown_timeout)

        self._key = key
        self._values = values

    def poll(self) -> KafkaMessage:
        try:
            value = next(self._values)
        except StopIteration:
            self.stop()
            return

        return self.serialize(key=self._key, value=value)


class KeyValueIterableSource(PollingSource):
    """
    Source implementation that iterate over an iterable of (key, value)

    Example Snippet:

    ```python
    from quixstreams import Application
    from quixstreams.sources import KeyValueIterableSource

    def messages():
        yield "one", 1
        yield "two", 2
        yield "three", 3
        yield "four", 4

    app = Application(broker_address='localhost:9092', consumer_group='group')
    source = KeyValueIterableSource(name="my_source", iterable=messages())

    sdf = app.dataframe(source=source)
    sdf.print()

    app.run(sdf)
    ```
    """

    def __init__(
        self,
        name: str,
        iterable: Iterable[Tuple[object, object]],
        shutdown_timeout: int = 10,
    ) -> None:
        """
        :param name: The source unique name. Used to generate the default topic
        :param iterable: The iterable for the (key, value) tuple.
        :param shutdown_timeout: Time in second the application waits for the source to gracefully shutdown
        """
        super().__init__(name, shutdown_timeout)

        self._iterable = iterable

    def poll(self) -> KafkaMessage:
        try:
            key, value = next(self._iterable)
        except StopIteration:
            self.stop()
            return

        return self.serialize(key=key, value=value)
