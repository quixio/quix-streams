import logging

from typing import Iterable, Optional, Tuple, Callable, Generator

from quixstreams.models.messages import KafkaMessage

from .base import PollingSource

logger = logging.getLogger(__name__)


__all__ = (
    "GeneratorSource",
    "ValueIterableSource",
    "KeyValueIterableSource",
)


class ValueIterableSource(PollingSource):
    """
    PollingSource implementation that iterate over values with a fix key.

    Example Snippet:

    ```python
    from quixstreams import Application
    from quixstreams.sources import ValueIterableSource

    app = Application(broker_address='localhost:9092', consumer_group='group')
    source = ValueIterableSource(name="my_source", values=range(10))

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
        self._values = iter(values)

    def poll(self) -> KafkaMessage:
        data = next(self._values)

        if data is None:
            return data

        return self.serialize(key=self._key, value=data)


class KeyValueIterableSource(PollingSource):
    """
    PollingSource implementation that iterate over an iterable of (key, value)

    Example Snippet:

    ```python
    from quixstreams import Application
    from quixstreams.sources import KeyValueIterableSource

    keys = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J"]
    app = Application(broker_address='localhost:9092', consumer_group='group')
    source = KeyValueIterableSource(name="my_source", iterable=zip(keys, range(10)))

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

        self._iterator = iter(iterable)

    def poll(self) -> KafkaMessage:
        data = next(self._iterator)

        if data is None:
            return data

        key, value = data
        return self.serialize(key=key, value=value)


class GeneratorSource(PollingSource):
    """
    PollingSource implementation that iterator over a generator of (key, value)

    Example Snippet:

    ```python
    from quixstreams import Application
    from quixstreams.sources import GeneratorSource

    def messages():
        yield "one", 1
        yield "two", 2
        yield "three", 3
        yield "four", 4

    app = Application(broker_address='localhost:9092', consumer_group='group')
    source = GeneratorSource(name="my_source", generator=messages)

    sdf = app.dataframe(source=source)
    sdf.print()

    app.run(sdf)
    ```
    """

    def __init__(
        self,
        name: str,
        generator: Callable[[], Generator[Optional[Tuple[any, any]], None, None]],
        polling_delay: float = 1,
        shutdown_timeout: float = 10,
    ) -> None:
        super().__init__(name, polling_delay, shutdown_timeout)

        self._generator = generator
        self._generator_instance: Optional[
            Generator[Optional[Tuple[any, any]], None, None]
        ] = None

    def run(self):
        self._generator_instance: Generator[Optional[Tuple[any, any]], None, None] = (
            self._generator()
        )
        super().run()

    def poll(self) -> KafkaMessage:
        data = next(self._generator_instance)

        if data is None:
            return data

        key, value = data
        return self.serialize(key=key, value=value)
