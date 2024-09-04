import logging

from typing import Iterator, Callable, Iterable

from .base import Source

logger = logging.getLogger(__name__)


__all__ = ("IterableSource",)


class IterableSource(Source):
    """
    Source implementation that iterate over an iterable returned by a callable.

    Example Snippet:

    ```python
    from quixstreams import Application
    from quixstreams.sources import IterableSource

    def messages():
        yield "key0", 0
        yield "key1", 1
        yield "key2", 2

    def main():
        app = Application()
        source = IterableSource(name="source", callable=messages.items)

        sdf = app.dataframe(source=source)
        sdf.print(metadata=True)

        app.run(sdf)

    if __name__ == "__main__":
        main()
    ```

    More examples

    ```python
    messages = {
        "key0": 0,
        "key1": 1,
        "key2": 2,
    }
    source = IterableSource(name="source", callable=messages.items)
    ```

    ```python
    messages = [("key0", 0), ("key1", 1), ("key2", 2)]
    source = IterableSource(name="source", callable=lambda: messages)
    ```

    """

    def __init__(
        self,
        name: str,
        callable: Callable[[], Iterable],
        key: str = "",
        shutdown_timeout: float = 10,
    ) -> None:
        super().__init__(name, shutdown_timeout)

        self._key = key
        self._callable = callable

    def _run(self):
        iterator: Iterator = iter(self._callable())

        while self.running:
            try:
                data = next(iterator)
            except StopIteration:
                return

            if self._key:
                key, value = self._key, data
            else:
                key, value = data

            msg = self.serialize(key=key, value=value)
            self.produce(
                value=msg.value,
                key=msg.key,
                headers=msg.headers,
                timestamp=msg.timestamp,
            )
