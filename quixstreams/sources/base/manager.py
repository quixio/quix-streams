import logging
import signal
import threading
from itertools import chain
from multiprocessing.context import SpawnProcess
from pickle import PicklingError
from typing import TYPE_CHECKING, List

from confluent_kafka import OFFSET_BEGINNING, TopicPartition

from quixstreams.internal_consumer import InternalConsumer
from quixstreams.internal_producer import InternalProducer
from quixstreams.logging import LOGGER_NAME, configure_logging
from quixstreams.models import Topic, TopicManager
from quixstreams.models.topics import TopicConfig
from quixstreams.state import RecoveryManager, StateStoreManager, StorePartition
from quixstreams.state.memory import MemoryStore

from .exceptions import SourceException
from .multiprocessing import multiprocessing
from .source import BaseSource, StatefulSource

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    process = SpawnProcess
else:
    process = multiprocessing.Process


class SourceProcess(process):
    """
    An implementation of the Source subprocess.

    It manages a source and its subprocess, handles the communication between the child and parent processes,
    lifecycle, and error handling.

    Some methods are designed to be used from the parent process, and others from the child process.
    """

    def __init__(
        self,
        source: BaseSource,
        topic: Topic,
        producer: InternalProducer,
        consumer: InternalConsumer,
        topic_manager: TopicManager,
    ) -> None:
        super().__init__()
        self.topic = topic
        self.source = source

        self._exceptions: List[Exception] = []
        self._started = False
        self._stopping = False

        self._topic_manager = topic_manager

        self._consumer = consumer
        self._producer = producer

        # copy parent process log level to the child process
        self._loglevel = logging.getLogger(LOGGER_NAME).level

        # reader and writer pipe used to communicate from the child to the parent process
        self._rpipe, self._wpipe = multiprocessing.Pipe(duplex=False)

    @property
    def started(self) -> bool:
        return self._started

    # --- CHILD PROCESS METHODS --- #

    def _setup_signal_handlers(self):
        """
        Configure the child process signal handlers to handle shutdown gracefully
        """
        signal.signal(signal.SIGINT, self._stop)
        signal.signal(signal.SIGTERM, self._stop)

    def run(self) -> None:
        """
        An entrypoint of the child process.

        Responsible for:
            * Configuring the signal handlers to handle shutdown properly
            * Execution of the source `run` method
            * Reporting the source exceptions to the parent process
        """
        self._started = True
        self._setup_signal_handlers()
        configure_logging(self._loglevel, str(self.source), pid=True)
        logger.info("Starting source")

        configuration = {}
        if isinstance(self.source, StatefulSource):
            try:
                configuration["store_partition"] = self._recover_state(self.source)
            except BaseException as err:
                logger.exception("Error in source")
                self._report_exception(err)
                return

        self.source.configure(
            topic=self.topic, producer=self._producer, **configuration
        )

        logger.info("Source started")
        try:
            self.source.start()
        except BaseException as err:
            logger.exception("Error in source")
            self._report_exception(err)
            return

        logger.info("Source completed")
        threadcount = threading.active_count()
        logger.debug(
            "%s active thread%s in source",
            threadcount,
            "s" if threadcount > 1 else "",
        )

    def _recover_state(self, source: StatefulSource) -> StorePartition:
        """
        Recover the state from the changelog topic and return the assigned partition

        For stateful sources only.
        """
        recovery_manager = RecoveryManager(
            consumer=self._consumer,
            topic_manager=self._topic_manager,
        )

        state_manager = StateStoreManager(
            producer=self._producer, recovery_manager=recovery_manager
        )

        state_manager.register_store(
            stream_id=None,
            store_name=source.store_name,
            store_type=MemoryStore,
            changelog_config=TopicConfig(
                num_partitions=source.store_partitions_count,
                replication_factor=self._topic_manager.default_replication_factor,
            ),
        )

        # Manually assign the changelog topic-partition for recovery
        changelog_topics = list(
            chain(*(d.values() for d in self._topic_manager.changelog_topics.values()))
        )
        changelog_tp = TopicPartition(
            topic=changelog_topics[0].name, partition=0, offset=OFFSET_BEGINNING
        )
        self._consumer.assign([changelog_tp])

        store_partitions = state_manager.on_partition_assign(
            stream_id=None,
            partition=source.assigned_store_partition,
            committed_offsets={},
        )

        if state_manager.recovery_required:
            state_manager.do_recovery()

        return store_partitions[source.store_name]

    def _stop(self, signum, _):
        """
        Stop the source execution.
        """
        signame = signal.Signals(signum).name
        logger.debug("Source received %s, stopping", signame)
        if self._stopping:
            return

        self._stopping = True

        try:
            self.source.stop()
        except BaseException as err:
            logger.exception("Error stopping source")
            self._report_exception(err)

    def _report_exception(self, err: BaseException) -> None:
        """
        Write an exception to the pipe so the parent process can access it.
        If the exception can't be pickled (for example confluent-kafka C exceptions) raise a RuntimeError.
        """
        try:
            self._wpipe.send(err)
        except (PicklingError, TypeError):
            self._wpipe.send(RuntimeError(str(err)))

    # --- PARENT PROCESS METHODS --- #

    def start(self) -> None:
        logger.info("Starting source %s", self.source)
        self._started = True
        super().start()

    def raise_for_error(self) -> None:
        """
        Raise a `quixstreams.sources.manager.SourceException`
        if the child process was terminated with an exception.
        """
        if not self.started:
            return

        if super().is_alive():
            return

        if not self._exceptions:
            while self._rpipe.poll():
                try:
                    self._exceptions.append(self._rpipe.recv())
                except EOFError:
                    return

        if self._exceptions:
            raise SourceException(self) from self._exceptions[-1]

        if self.exitcode not in (0, -signal.SIGTERM):
            raise SourceException(self)

    def stop(self):
        """
        Handle shutdown of the source and its subprocess.

        First, it tries to shut down gracefully by sending a SIGTERM and waiting up to
        `source.shutdown_timeout` seconds for the process to exit. If the process
        is still alive, it will kill it with a SIGKILL.
        """
        if self.is_alive():
            logger.info("Stopping source %s", self.source)
            self.terminate()
            self.join(self.source.shutdown_timeout)

        if self.is_alive():
            logger.error(
                "Shutdown timeout (%ss) reached. Force stopping source %s",
                self.source.shutdown_timeout,
                self.source,
            )
            self.kill()
            self.join(self.source.shutdown_timeout)


class SourceManager:
    """
    Class managing the sources registered with the app

    Sources run in their separate process pay attention about cross-process communication
    """

    def __init__(self) -> None:
        self.processes: List[SourceProcess] = []

    def register(
        self,
        source: BaseSource,
        topic,
        producer,
        consumer,
        topic_manager,
    ) -> SourceProcess:
        """
        Register a new source in the manager.

        Each source need to already be configured, can't reuse a topic and must be unique
        """
        if topic in self.topics:
            raise ValueError(f'Topic name "{topic.name}" is already in use')
        elif source in self.sources:
            raise ValueError(f'Source "{source}" is already registered')

        process = SourceProcess(
            source=source,
            topic=topic,
            producer=producer,
            consumer=consumer,
            topic_manager=topic_manager,
        )
        self.processes.append(process)
        return process

    @property
    def sources(self) -> List[BaseSource]:
        return [process.source for process in self.processes]

    @property
    def topics(self) -> List[Topic]:
        return [process.topic for process in self.processes]

    def start_sources(self) -> None:
        for process in self.processes:
            if not process.started:
                process.start()

    def stop_sources(self) -> None:
        for process in self.processes:
            process.stop()

        try:
            self.raise_for_error()
        finally:
            for process in self.processes:
                process.close()

    def raise_for_error(self) -> None:
        """
        Raise an exception if any process has stopped with an exception
        """
        for process in self.processes:
            process.raise_for_error()

    def is_alive(self) -> bool:
        """
        Check if any process is alive

        :return: True if at least one process is alive
        """
        for process in self.processes:
            try:
                if process.is_alive():
                    return True
            except ValueError:
                continue

        return False

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.stop_sources()
