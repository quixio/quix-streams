import logging
from time import monotonic, sleep
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from confluent_kafka import KafkaError, KafkaException, Message, TopicPartition
from confluent_kafka.admin import GroupMetadata

from quixstreams.exceptions import QuixException

from .error_callbacks import ProducerErrorCallback, default_on_producer_error
from .kafka.configuration import ConnectionConfig
from .kafka.exceptions import KafkaProducerDeliveryError
from .kafka.producer import (
    PRODUCER_ON_ERROR_RETRIES,
    PRODUCER_POLL_TIMEOUT,
    Producer,
)
from .models import Headers, Row, Topic

logger = logging.getLogger(__name__)


_KEY_UNSET = object()

# Retry policy for transactional Kafka ops (abort / send-offsets / commit).
# confluent-kafka's contract says a *retriable* error (e.g. _TIMED_OUT during a
# transient coordinator outage) should be retried by calling the op again. See
# InternalProducer._retry_transaction_op for how the attempt cap and the
# optional wall-clock budget interact (a bounded caller, the revoke path inside
# the rebalance callback, can never block past its budget).
_ABORT_RETRY_ATTEMPTS = 3
_ABORT_RETRY_BACKOFF = 1.0

# librdkafka delivery-report error codes fired when purge()/abort_transaction()
# drops queued or in-flight messages. These are NOT real delivery failures (the
# messages were intentionally discarded and nothing from the purged/aborted
# batch was committed), so _on_delivery filters them out instead of poisoning
# _error, which the next produce()/flush() would otherwise raise.
_PURGE_ERROR_CODES = frozenset(
    {
        KafkaError._PURGE_QUEUE,  # noqa: SLF001
        KafkaError._PURGE_INFLIGHT,  # noqa: SLF001
    }
)


def _deadline_from_timeout(timeout: Optional[float]) -> Optional[float]:
    """
    Absolute ``time.monotonic()`` deadline ``timeout`` seconds from now, or
    ``None`` (unbounded) when ``timeout`` is ``None`` or negative (the ``-1.0``
    librdkafka "infinite" sentinel). Shared by the abort and commit paths.
    """
    if timeout is None or timeout < 0:
        return None
    return monotonic() + timeout


class KafkaProducerTransactionCommitFailed(QuixException): ...


class KafkaProducerTransactionAlreadyActive(QuixException):
    """Raised when ``begin_transaction()`` hits librdkafka's ``_STATE`` error
    because a transaction is already open: a previous checkpoint left its
    transaction dangling (neither committed nor aborted)."""


class InternalProducer:
    """
    A producer class that is capable of serializing Rows to bytes and send them to Kafka.
    The serialization is performed according to the Topic serialization settings.

    :param broker_address: Connection settings for Kafka.
        Accepts string with Kafka broker host and port formatted as `<host>:<port>`,
        or a ConnectionConfig object if authentication is required.
    :param extra_config: A dictionary with additional options that
        will be passed to `confluent_kafka.Producer` as is.
        Note: values passed as arguments override values in `extra_config`.
    :param on_error: a callback triggered when `InternalProducer.produce_row()`
        or `InternalProducer.poll()` fail`.
        If producer fails and the callback returns `True`, the exception
        will be logged but not propagated.
        The default callback logs an exception and returns `False`.
    :param flush_timeout: The time the producer is waiting for all messages to be delivered.
    :param transactional: whether to use Kafka transactions or not.
        Note this changes which underlying `Producer` class is used.
    """

    def __init__(
        self,
        broker_address: Union[str, ConnectionConfig],
        extra_config: Optional[dict] = None,
        on_error: Optional[ProducerErrorCallback] = None,
        flush_timeout: Optional[float] = None,
        transactional: bool = False,
    ):
        self._producer = Producer(
            broker_address=broker_address,
            extra_config=extra_config,
            flush_timeout=flush_timeout,
            transactional=transactional,
        )

        self._on_error: ProducerErrorCallback = on_error or default_on_producer_error
        self._tp_offsets: Dict[Tuple[str, int], int] = {}
        self._error: Optional[KafkaError] = None
        self._active_transaction = False

    def produce_row(
        self,
        row: Row,
        topic: Topic,
        key: Optional[Any] = _KEY_UNSET,
        partition: Optional[int] = None,
        timestamp: Optional[int] = None,
    ):
        """
        Serialize Row to bytes according to the Topic serialization settings
        and produce it to Kafka

        If this method fails, it will trigger the provided "on_error" callback.

        :param row: Row object
        :param topic: Topic object
        :param key: message key, optional
        :param partition: partition number, optional
        :param timestamp: timestamp in milliseconds, optional
        """

        try:
            # Use existing key only if no other key is provided.
            # If key is provided - use it, even if it's None
            key = row.key if key is _KEY_UNSET else key
            message = topic.row_serialize(row=row, key=key)
            self.produce(
                topic=topic.name,
                key=message.key,
                value=message.value,
                headers=message.headers,
                partition=partition,
                timestamp=timestamp,
            )
        except Exception as exc:
            to_suppress = self._on_error(exc, row, logger)
            if to_suppress:
                return
            raise

    def poll(self, timeout: float = 0):
        """
        Polls the producer for events and calls `on_delivery` callbacks.

        If `poll()` fails, it will trigger the provided "on_error" callback

        :param timeout: timeout in seconds
        """
        try:
            self._producer.poll(timeout=timeout)
        except Exception as exc:
            to_suppress = self._on_error(exc, None, logger)
            if to_suppress:
                return
            raise

    def _broker_available(self):
        """Reset the broker unavailability tracker on the underlying Producer."""
        self._producer._broker_available()  # noqa: SLF001

    def raise_if_broker_unavailable(self, timeout: float):
        """Raise if all brokers have been unavailable for longer than ``timeout`` seconds."""
        self._producer.raise_if_broker_unavailable(timeout)

    def produce(
        self,
        topic: str,
        value: Optional[Union[str, bytes]] = None,
        key: Optional[Union[str, bytes]] = None,
        headers: Optional[Headers] = None,
        partition: Optional[int] = None,
        timestamp: Optional[int] = None,
        poll_timeout: float = PRODUCER_POLL_TIMEOUT,
        buffer_error_max_tries: int = PRODUCER_ON_ERROR_RETRIES,
        on_delivery: Optional[Callable[[Optional[KafkaError], Message], None]] = None,
    ):
        self._raise_for_error()

        # An optional caller-supplied delivery callback (e.g. the legacy-TTL
        # backfill's per-partition ack counter, review batch 3 #5) is CHAINED with
        # the internal ``_on_delivery`` — never replaces it — so offset tracking and
        # error capture keep working. librdkafka invokes exactly one callback per
        # record, so we combine them here.
        delivery: Callable[[Optional[KafkaError], Message], None] = self._on_delivery
        if on_delivery is not None:
            caller_on_delivery = on_delivery

            def delivery(err: Optional[KafkaError], msg: Message) -> None:
                self._on_delivery(err, msg)
                caller_on_delivery(err, msg)

        return self._producer.produce(
            topic=topic,
            value=value,
            key=key,
            headers=headers,
            partition=partition,
            timestamp=timestamp,
            poll_timeout=poll_timeout,
            buffer_error_max_tries=buffer_error_max_tries,
            on_delivery=delivery,
        )

    def _on_delivery(self, err: Optional[KafkaError], msg: Message):
        if err is not None:
            # Purge reports (from purge()/abort_transaction() dropping queued or
            # in-flight messages) are not real delivery failures: nothing from a
            # purged/aborted batch was committed, so they must not surface on the
            # next produce()/flush(). Filtering them here removes the need to
            # snapshot/restore _error around every purge/abort -- a dance that
            # could discard a genuine error firing in the same poll window.
            if err.code() in _PURGE_ERROR_CODES:
                return
            if self._error is None:
                self._error = err
            return

        if self._error is not None:
            # A real error is already latched; stop recording offsets until it is
            # surfaced by _raise_for_error (fail-fast).
            return

        topic, partition, offset = msg.topic(), msg.partition(), msg.offset()
        self._tp_offsets[(topic, partition)] = offset
        # Successful delivery confirms broker is reachable
        self._producer._broker_available()  # noqa: SLF001

    def _raise_for_error(self):
        if self._error is not None:
            exc = KafkaProducerDeliveryError(self._error)
            self._error = None
            raise exc

    def flush(self, timeout: Optional[float] = None) -> int:
        result = self._producer.flush(timeout=timeout)
        self._raise_for_error()
        return result

    def purge(self):
        """
        Purge all messages currently queued or in-flight in the producer.

        Used on the revoke path when a bounded flush could not confirm changelog
        delivery: the still-queued changelog/output messages cannot deliver after
        the partition is handed to the new owner. Already-transmitted in-flight
        requests may still be appended by the broker (their acknowledgements are
        only voided locally), so this reduces but does not fully eliminate zombie
        writes landing behind the new owner's own writes.

        Swallowing the purge-induced delivery errors is safe here: offsets were
        NOT committed, so the new owner reprocesses these messages from the last
        committed offset (at-least-once). The ``_PURGE`` reports are filtered by
        ``_on_delivery``, so a genuine delivery error recorded before the purge
        still surfaces on the next checkpoint.
        """
        self._producer.purge()
        # Drain the _PURGE delivery reports purge() just enqueued; _on_delivery
        # filters those codes, so _error stays clean.
        self._producer.poll(0)

    @property
    def offsets(self) -> Dict[Tuple[str, int], int]:
        return self._tp_offsets

    def begin_transaction(self):
        try:
            self._producer.begin_transaction()
        except KafkaException as exc:
            # Only the opaque _STATE case (a transaction is already open) gets
            # the named invariant error. A fatal error like _FENCED leaves
            # _active_transaction legitimately True, and its truthful librdkafka
            # code must still propagate unwrapped.
            if (
                self._active_transaction and exc.args[0].code() == KafkaError._STATE  # noqa: SLF001
            ):
                raise KafkaProducerTransactionAlreadyActive(
                    "begin_transaction() called while a transaction is already "
                    "active: a previous checkpoint left its transaction dangling "
                    "(not committed or aborted)."
                ) from exc
            raise
        self._active_transaction = True

    def _retry_transaction_op(
        self,
        op: Callable[[Optional[float]], None],
        *,
        op_name: str,
        deadline: Optional[float],
        max_attempts: Optional[int],
        on_exhausted: Optional[Callable[[str], Exception]] = None,
    ) -> None:
        """
        Run a transactional Kafka op, retrying on *retriable* errors.

        Shared retriable/backoff core for ``abort_transaction`` and
        ``commit_transaction`` (send-offsets + commit). confluent-kafka's
        contract says a retriable error (e.g. ``_TIMED_OUT`` during a transient
        coordinator outage) should be retried by calling the op again; a
        non-retriable / fenced / fatal error propagates immediately, unchanged.

        :param op: callable taking a per-attempt timeout (seconds, or ``None``
            for librdkafka "infinite") and performing one attempt.
        :param op_name: name for logging / the exhaustion exception.
        :param deadline: absolute ``time.monotonic()`` deadline for the WHOLE op
            (every attempt plus backoff), or ``None`` for no wall-clock bound.
            Each attempt is given the whole remaining budget, so a genuine
            timeout consumes it and is not retried past the deadline. Bounded
            callers (the revoke path, inside the rebalance callback) rely on this
            to guarantee the total time never exceeds their budget.
        :param max_attempts: cap on the number of attempts, or ``None`` for no
            cap -- retry retriable errors indefinitely (the idle/close abort's
            "wait out a transient coordinator blip" contract; only combined with
            ``deadline=None``).
        :param on_exhausted: builds the exception raised once the attempt cap or
            the budget is exhausted after a retriable error. When ``None`` the
            last retriable error is re-raised (an exception after a bounded time
            -- no worse than not retrying at all).
        """
        attempt = 0
        while True:
            if deadline is not None:
                op_timeout: Optional[float] = max(0.0, deadline - monotonic())
            else:
                # None -> Producer maps to librdkafka -1 (block until done/fatal).
                op_timeout = None
            try:
                op(op_timeout)
                return
            except KafkaException as exc:
                error = exc.args[0]
                if not error.retriable():
                    # Fatal / fenced / non-retriable: propagate unchanged.
                    logger.error(f"Error during Kafka transaction operation {op_name}")
                    raise
                attempt += 1
                attempts_left = max_attempts is None or attempt < max_attempts
                budget_left = deadline is None or (deadline - monotonic()) > 0
                if not attempts_left or not budget_left:
                    if on_exhausted is not None:
                        raise on_exhausted(op_name) from exc
                    raise
                logger.debug(
                    f"Retriable error during Kafka transaction operation "
                    f"{op_name}; retrying (attempt {attempt})."
                )
                backoff = _ABORT_RETRY_BACKOFF
                if deadline is not None:
                    backoff = min(backoff, max(0.0, deadline - monotonic()))
                if backoff > 0:
                    sleep(backoff)

    def abort_transaction(self, timeout: Optional[float] = None):
        """
        Attempt an abort if an active transaction.

        Else, skip since it throws an exception if at least
        one transaction was successfully completed at some point.

        This avoids polluting the stack trace in the case where a transaction was
        not active as expected (because of some other exception already raised)
        and a cleanup abort is attempted.

        NOTE: under normal circumstances a transaction will be open due to how
        the Checkpoint inits another immediately after committing.

        ``timeout`` is the OVERALL wall-clock budget (seconds) for the whole
        abort, retries included:

        * A finite ``timeout`` (the revoke path, inside the rebalance callback)
          bounds both the budget AND the attempts, so it can never block past
          ``max.poll.interval.ms``.
        * ``None`` (or negative) means "no deadline" -- a retriable error is
          retried indefinitely per confluent-kafka's "call abort_transaction()
          again to continue the abort" contract, waiting out a transient
          coordinator outage instead of crashing on the first timeout. Callers
          off the rebalance path pass a finite idle/shutdown budget instead of
          relying on this (see ``Checkpoint._abort_transaction_if_eos``).

        On success ``_active_transaction`` is cleared; a non-retriable / fenced /
        fatal error, or retriable exhaustion of a finite budget, propagates and
        leaves ``_active_transaction`` ``True`` (the transaction is still open).
        """
        if not self._active_transaction:
            logger.debug("No Kafka transaction to abort")
            return
        if self._tp_offsets:
            # Only log here to avoid polluting logging with empty checkpoint aborts
            logger.debug("Aborting Kafka transaction and clearing producer offsets")
            self._tp_offsets = {}

        # A finite timeout (revoke path) yields a deadline and caps the attempts;
        # None/negative (idle / close / shutdown) yields no deadline and no
        # attempt cap -> retry a retriable error indefinitely (wait out a blip).
        deadline = _deadline_from_timeout(timeout)
        self._retry_transaction_op(
            self._producer.abort_transaction,
            op_name="abort_transaction",
            deadline=deadline,
            max_attempts=_ABORT_RETRY_ATTEMPTS if deadline is not None else None,
            on_exhausted=None,
        )
        # librdkafka's abort purges queued messages and flushes in-flight ones,
        # firing _PURGE_QUEUE/_PURGE_INFLIGHT delivery reports through
        # _on_delivery on this thread. Drain them now; _on_delivery filters those
        # codes, so they never poison _error (no snapshot/restore dance needed).
        self._producer.poll(0)
        self._active_transaction = False

    def commit_transaction(
        self,
        positions: List[TopicPartition],
        group_metadata: GroupMetadata,
        timeout: Optional[float] = None,
    ):
        """
        Send the consumer offsets into the open transaction and commit it.

        Some failure cases from sending offsets or committing a transaction are
        retriable, which is worth re-attempting since the transaction is almost
        complete (the changelog was flushed before attempting to commit).

        ``timeout`` is the OVERALL wall-clock budget (seconds) shared by BOTH the
        send-offsets and the commit steps, so the whole operation is bounded by
        one budget rather than 2x it -- important on the revoke path, inside the
        rebalance callback. ``None`` (off the revoke path) means unbounded: each
        step keeps its legacy behavior (retry up to ``_ABORT_RETRY_ATTEMPTS``
        times, then raise ``KafkaProducerTransactionCommitFailed`` to trigger the
        Application shutdown).
        """
        deadline = _deadline_from_timeout(timeout)

        def _fail(op_name: str) -> Exception:
            return KafkaProducerTransactionCommitFailed(
                f"All Kafka {op_name} attempts failed; "
                "aborting transaction and shutting down Application..."
            )

        self._retry_transaction_op(
            lambda t: self._producer.send_offsets_to_transaction(
                positions, group_metadata, t
            ),
            op_name="send_offsets_to_transaction",
            deadline=deadline,
            max_attempts=_ABORT_RETRY_ATTEMPTS,
            on_exhausted=_fail,
        )
        self._retry_transaction_op(
            self._producer.commit_transaction,
            op_name="commit_transaction",
            deadline=deadline,
            max_attempts=_ABORT_RETRY_ATTEMPTS,
            on_exhausted=_fail,
        )
        self._active_transaction = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush()
