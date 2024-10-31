import logging
import signal
import threading
from pickle import PicklingError
from typing import List

from quixstreams.logging import LOGGER_NAME, configure_logging
from quixstreams.models import Topic

from .exceptions import SourceException
from .multiprocessing import multiprocessing
from .source import BaseSource

logger = logging.getLogger(__name__)


class SourceProcess(multiprocessing.Process):
    """
    An implementation of the Source subprocess.

    It manages a source and its subprocess, handles the communication between the child and parent processes,
    lifecycle, and error handling.

    Some methods are designed to be used from the parent process, and others from the child process.
    """

    def __init__(self, source):
        super().__init__()
        self.source: BaseSource = source

        self._exceptions: List[Exception] = []
        self._started = False
        self._stopping = False

        # copy parent process log level to the child process
        self._loglevel = logging.getLogger(LOGGER_NAME).level

        # reader and writer pipe used to communicate from the child to the parent process
        self._rpipe, self._wpipe = multiprocessing.Pipe(duplex=False)

    @property
    def started(self):
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
        logger.info("Source started")

        try:
            self.source.start()
        except BaseException as err:
            logger.exception("Error in source")
            self._report_exception(err)

        logger.info("Source completed")
        threadcount = threading.active_count()
        logger.debug(
            "%s active thread%s in source",
            threadcount,
            "s" if threadcount > 1 else "",
        )

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

    def start(self) -> "SourceProcess":
        logger.info("Starting source %s", self.source)
        self._started = True
        return super().start()

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

    def __init__(self):
        self.processes: List[SourceProcess] = []

    def register(self, source: BaseSource):
        """
        Register a new source in the manager.

        Each source need to already be configured, can't reuse a topic and must be unique
        """
        if not source.configured:
            raise ValueError("Accepts configured Source only")
        if source.producer_topic in self.topics:
            raise ValueError(f"topic '{source.producer_topic.name}' already in use")
        elif source in self.sources:
            raise ValueError(f"source '{source}' already registered")

        process = SourceProcess(source)
        self.processes.append(process)
        return process

    @property
    def sources(self) -> List[BaseSource]:
        return [process.source for process in self.processes]

    @property
    def topics(self) -> List[Topic]:
        return [process.source.producer_topic for process in self.processes]

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
