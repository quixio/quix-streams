import threading
import logging
import signal
import multiprocessing
import multiprocessing.queues

from pickle import PicklingError
from typing import List, Optional

from quixstreams.models import Topic
from .base import BaseSource

logger = logging.getLogger(__name__)


class SourceProcess(multiprocessing.Process):
    """
    An individual source process

    Used to manage a source and it's process. Handles communication accross the child and parent processes,
    lifecycle, error handling.

    Part of the methods are designed to be used from the parent process and other from the child process.
    """

    def __init__(self, source):
        super().__init__()
        self.source: BaseSource = source

        self._exception: Optional[Exception] = None
        self._stopping = False

        # reader and writer pipe used to communicate from the child to the parent process
        self._rpipe, self._wpipe = multiprocessing.Pipe(duplex=False)

    # --- CHILD PROCESS METHODS --- #

    def _setup_signal_handlers(self):
        """
        Configure the child process signal handlers to handle shutdown gracefully
        """
        signal.signal(signal.SIGINT, self._stop)
        signal.signal(signal.SIGTERM, self._stop)

    def run(self) -> None:
        """
        Entrypoing of the child process

        Responsible for:
            * Configuring the signal handlers to properly handle shutdown
            * Execute the source `run` method
            * Report the source exception to the parent process
            * Execute cleanup
        """
        self._setup_signal_handlers()
        logger.info("Source %s started with PID %s", self.source, self.pid)

        try:
            self.source.run()
        except BaseException as err:
            logger.exception(f"Error in source %s", self)
            self._report_exception(err)
            self._cleanup(failed=True)
        else:
            self._cleanup(failed=False)

        self._wpipe.close()
        logger.info("Source %s with PID %s completed", self.source, self.pid)

        threadcount = threading.active_count()
        logger.debug(
            "%s active thread%s in source %s with PID %s",
            threadcount,
            "s" if threadcount > 1 else "",
            self.source,
            self.pid,
        )

    def _cleanup(self, failed: bool) -> None:
        """
        Execute post-run cleanup
        """
        logger.debug("Running cleanup on source %s with PID %s", self.source, self.pid)
        try:
            self.source.cleanup(failed)
        except BaseException as err:
            logger.exception(f"Error cleaning up source %s", self)
            self._report_exception(err)

    def _stop(self, *args):
        """
        Ask the source to stop execution
        """
        if self._stopping:
            return

        self._stopping = True
        logger.debug(
            "Source %s with PID %s received SIGINT, stopping", self.source, self.pid
        )

        try:
            self.source.stop()
        except BaseException as err:
            logger.exception(f"Error stopping source %s", self)
            self._report_exception(err)

    def _report_exception(self, err: Exception) -> None:
        """
        Write exception to the pipe so the parent process can access it.
        If the exception can't be pickled (for example confluent-kafka C exceptions) raise a RuntimeError.
        """
        try:
            self._wpipe.send(err)
        except (PicklingError, TypeError):
            self._wpipe.send(RuntimeError(str(err)))

    # --- PARENT PROCESS METHODS --- #

    def start(self) -> "SourceProcess":
        logger.info("Starting source %s", self.source)
        return super().start()

    def raise_for_error(self) -> bool:
        """
        If the child process has terminated with an exception raises a
        :class:`quixstreams.sources.manager.SourceException`.
        """
        if super().is_alive():
            return

        if self._exception is None:
            if self._rpipe.poll():
                try:
                    self._exception = self._rpipe.recv()
                except EOFError:
                    return

        if self._exception is not None:
            raise SourceException(self) from self._exception

        if self.exitcode != 0:
            raise SourceException(self)

    def stop(self):
        """
        Handle shutdown of the source and the associated process

        First try a graceful shutdown by sending a SIGTERM and waiting up to
        `source.shutdown_timeout` seconds for the process to exit. If the process
        is still alive force kill it with a SIGKILL/
        """
        if self.is_alive():
            logger.info("Stopping source %s with PID %s", self.source, self.pid)
            self.terminate()
            self.join(self.source.shutdown_timeout)

        if self.is_alive():
            logger.info("Force stopping source %s with PID %s", self.source, self.pid)
            self.kill()
            self.join(self.source.shutdown_timeout)

        self.close()


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

        self.processes.append(SourceProcess(source))

    @property
    def sources(self) -> List[BaseSource]:
        return [process.source for process in self.processes]

    @property
    def topics(self) -> List[Topic]:
        return [process.source.producer_topic for process in self.processes]

    def start_sources(self) -> None:
        for process in self.processes:
            process.start()

    def stop_sources(self) -> None:
        for process in self.processes:
            process.stop()

    def raise_for_error(self) -> None:
        """
        Raise an exception if any process has stopped with an exception
        """
        for process in self.processes:
            process.raise_for_error()

    def alives(self) -> bool:
        """
        Check if any process is alive

        :return: True if at least one process is alive
        """
        return any(process.is_alive() for process in self.processes)

    def __enter__(self):
        self.start_sources()
        return self

    def __exit__(self, *args, **kwargs):
        self.stop_sources()


class SourceException(Exception):
    """
    Raised in the parent process when a source finish with an exception
    """

    def __init__(self, process: SourceProcess) -> None:
        self.pid: int = process.pid
        self.process: SourceProcess = process
        self.exitcode = self.process.exitcode

    def __str__(self) -> str:
        msg = f"{self.process.source} with PID {self.pid} failed"
        if self.exitcode == 0:
            return msg
        return f"{msg} with exitcode {self.exitcode}"
