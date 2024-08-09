import logging
import threading

from typing import List, Dict

from quixstreams.models import Topic
from quixstreams.rowproducer import RowProducer

from .base import SourceStoppingException, BaseSource

logger = logging.getLogger(__name__)


class SourceThread(threading.Thread):
    """
    Class managing the lifecycle of a source inside a thread.
    """

    def __init__(self, source, topic, producer):
        super().__init__()
        self.source: BaseSource = source
        self.topic: Topic = topic
        self.producer: RowProducer = producer

        self._running = False

    def start(self) -> "SourceThread":
        logger.info("starting source %s", self.source)
        self.source.configure(self.topic, self.producer)
        return super().start()

    def run(self) -> None:
        self._running = True
        try:
            self.source.run()
        except SourceStoppingException:
            raise
        except BaseException:
            logger.exception(f"Error in source {self}")
            raise

        if self._running:
            self.stop()

    def stop(self):
        if self._running:
            self._running = False
            self.source.stop()

    def wait_stopped(self) -> None:
        """
        Wait, up to `source.shutdown_timeout` seconds, for the thread to exit.

        :raises: TimeoutError if the thread is still alive after the shutdown timeout
        """
        self.join(self.source.shutdown_timeout)
        if self.is_alive():
            raise TimeoutError(f"source '{self.source}' failed to shutdown gracefully")


class SourceManager:
    def __init__(self):
        self.threads: List[SourceThread] = []

    def register(self, source: BaseSource, producer: RowProducer, topic: Topic):
        if topic in self.topics:
            raise ValueError(f"topic '{topic.name}' already in use")
        elif source in self.sources:
            raise ValueError(f"source '{source}' already registered")

        self.threads.append(SourceThread(source, topic, producer))

    @property
    def sources(self) -> List[BaseSource]:
        return [thread.source for thread in self.threads]

    @property
    def topics(self) -> List[Topic]:
        return [thread.topic for thread in self.threads]

    def checkpoint(self):
        for thread in self.threads:
            thread.source.checkpoint()

    def start_sources(self):
        for thread in self.threads:
            thread.start()

    def stop_sources(self):
        for thread in self.threads:
            thread.stop()

        for thread in self.threads:
            thread.wait_stopped()
