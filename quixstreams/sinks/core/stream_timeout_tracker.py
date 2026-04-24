"""
Stream-timeout tracker for sinks.

Provides ``StreamTimeoutTracker`` — a sink-agnostic, per-key silence
detector that fires a user callback when a tracked "stream" (typically a
Kafka message key) has been silent past a configurable threshold.

The tracker owns all state and threading for the feature; host sinks
compose it via four one-liners (``touch`` on add, ``check_now`` on
flush, ``start`` on setup, ``stop`` on cleanup). See spec v6 of the
Quix Lake sink timeout feature for the behavioural contract that this
module implements verbatim.

This module intentionally has **zero imports** from any specific sink,
from ``quixstreams.sinks.core``, or from any Quix-platform-specific
type. Only the standard library is used so any sink (core, community,
or third-party) can drop it in.
"""

import logging
import threading
import time
from typing import Any, Callable, Optional


class StreamTimeoutTracker:
    """Per-key silence detector, extracted from ``QuixTSDataLakeSink``.

    One "stream" = one key. Callers invoke :meth:`touch` every time a
    record arrives for a key; the tracker records the last-seen wall
    clock time and fires the configured callback exactly once per
    silence period per key. On fire the key is evicted — a later
    :meth:`touch` for the same key resurrects it as a fresh stream
    eligible to fire again.

    Keys are stored and surfaced **as-is** (raw pass-through). Any
    hashable value is accepted: ``str``, ``bytes``, ``int``, etc. The
    callback receives the exact object that was passed to
    :meth:`touch`. ``None`` keys are silently skipped.

    The tracker exposes a property :attr:`enabled`. When ``False``,
    every public method is a no-op and no per-key dict is allocated.

    Two code paths drive checks:

    - :meth:`check_now`, called synchronously by the host sink at the
      end of each flush.
    - A background daemon thread started by :meth:`start` that runs
      :meth:`check_now` on a periodic cadence. The thread
      **self-terminates** after ``idle_exit_cycles`` consecutive empty
      cycles and is **respawned** by the next :meth:`touch` that
      records a new stamp. This keeps the sink idle-zero-overhead
      when no keys are tracked.

    Concurrency: :meth:`touch` and :meth:`check_now` can run on
    different threads. A single ``threading.Lock`` guards the per-key
    dict and timer-thread reference. Critical sections are tiny; user
    callbacks are invoked **outside** the lock so a blocking callback
    cannot stall :meth:`touch`.
    """

    def __init__(
        self,
        stream_timeout_ms: Optional[int],
        on_stream_timeout: Optional[Callable[[Any], None]],
        *,
        check_interval_ms: Optional[int] = None,
        idle_exit_cycles: int = 3,
        thread_name: str = "StreamTimeoutTracker-check",
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Construct the tracker.

        :param stream_timeout_ms: Positive ``int`` (milliseconds) to
            enable; ``None`` to disable. ``bool`` is rejected
            explicitly. Non-positive values raise ``ValueError`` when
            paired with a callable callback.
        :param on_stream_timeout: Callable ``(stream_key: Any) -> None``
            to enable; ``None`` to disable. Mismatched pair (exactly
            one ``None``) raises ``ValueError``. The callback receives
            the raw key object that was passed to :meth:`touch`.
        :param check_interval_ms: Kw-only. Override the periodic
            cadence. Positive ``int`` wins; anything else falls back
            to ``max(100, min(1000, stream_timeout_ms // 5))``.
        :param idle_exit_cycles: Kw-only. Consecutive empty-tracker
            cycles before the timer thread self-terminates.
        :param thread_name: Kw-only. Name for the daemon thread.
        :param logger: Kw-only. Logger to emit INFO/WARNING/EXCEPTION
            lines on. Defaults to this module's logger.
        """
        self._logger = logger or logging.getLogger(__name__)
        self._thread_name = thread_name

        self._enabled: bool = False

        self._validate_stream_timeout_params(stream_timeout_ms, on_stream_timeout)

        if (
            isinstance(stream_timeout_ms, int)
            and not isinstance(stream_timeout_ms, bool)
            and stream_timeout_ms > 0
            and callable(on_stream_timeout)
        ):
            self._enabled = True
            self._stream_timeout_ms: int = stream_timeout_ms
            self._on_stream_timeout: Callable[[Any], None] = on_stream_timeout
            # Per-stream last-seen timestamps. Keys are raw Kafka message
            # keys (any hashable — str, bytes, int, etc.); values are
            # wall-clock ms from ``_now_ms()``. Bounded in practice by
            # eviction-on-fire and the 3x TTL safety sweep inside
            # ``check_now()``.
            self._last_seen_by_stream: dict[Any, int] = {}
            self._tracker_lock: threading.Lock = threading.Lock()
            self._stop: threading.Event = threading.Event()

            if (
                isinstance(check_interval_ms, int)
                and not isinstance(check_interval_ms, bool)
                and check_interval_ms > 0
            ):
                self._check_interval_ms: int = check_interval_ms
            else:
                self._check_interval_ms = max(100, min(1000, stream_timeout_ms // 5))

            self._idle_exit_cycles: int = idle_exit_cycles
            self._timer_thread: Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_stream_timeout_params(
        stream_timeout_ms: Any, on_stream_timeout: Any
    ) -> None:
        """Validate the two scalar stream-timeout params.

        Policy:
        - Both ``None`` -> disabled silently, no error.
        - Exactly one supplied in a non-``None`` form -> raise with
          the pair message so operators catch "set the threshold but
          forgot the callback" loudly at construction time.
        - ``stream_timeout_ms`` provided alongside a callable callback
          but not a positive int -> raise with the threshold message.
        - ``on_stream_timeout`` provided alongside a positive int
          threshold but not callable -> raise with the callback
          message.
        """
        if stream_timeout_ms is None and on_stream_timeout is None:
            return

        if (stream_timeout_ms is None) != (on_stream_timeout is None):
            raise ValueError(
                "stream_timeout_ms and on_stream_timeout must both be "
                "provided to enable stream-timeout tracking; got "
                f"stream_timeout_ms={stream_timeout_ms!r}, "
                f"on_stream_timeout={on_stream_timeout!r}"
            )

        if (
            not isinstance(stream_timeout_ms, int)
            or isinstance(stream_timeout_ms, bool)
            or stream_timeout_ms <= 0
        ):
            raise ValueError("stream_timeout_ms must be a positive int (milliseconds)")
        if not callable(on_stream_timeout):
            raise ValueError("on_stream_timeout must be callable")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def enabled(self) -> bool:
        """``True`` iff the feature is active (both params valid)."""
        return self._enabled

    def touch(
        self,
        stream_key: Any,
        *,
        now_ms: Optional[int] = None,
        **log_context: Any,
    ) -> None:
        """Refresh the last-seen timestamp for ``stream_key``.

        The key is stored as-is (raw pass-through); any hashable value
        works (``str``, ``bytes``, ``int``, etc.). ``None`` keys are
        silently skipped.

        Callers may pass ``topic``/``partition``/``offset`` (or any
        other context) as keyword arguments; these are accepted for
        call-site compatibility with earlier versions of the tracker
        and are currently unused. The tracker remains sink-agnostic —
        it treats the context as opaque kwargs.

        :param stream_key: The raw key from the record. Must be
            hashable if not ``None``.
        :param now_ms: Kw-only. Explicit timestamp in milliseconds;
            defaults to :meth:`_now_ms`.
        :param log_context: Kw-only. Reserved for future per-record
            log enrichment; currently ignored.
        """
        if not self._enabled:
            return
        if stream_key is None:
            return
        ts = now_ms if now_ms is not None else self._now_ms()
        with self._tracker_lock:
            self._last_seen_by_stream[stream_key] = ts
            self._ensure_timer_thread_alive()

    def check_now(self, *, now_ms: Optional[int] = None) -> None:
        """Run one pass of the silence check.

        Fires and evicts keys whose silence >= threshold; silently
        drops keys whose silence >= ``3 * stream_timeout_ms`` (TTL
        safety sweep). Callbacks run outside the tracker lock; a
        callback that raises leaves its key in the tracker for retry
        on the next cycle.
        """
        if not self._enabled:
            return

        ts = now_ms if now_ms is not None else self._now_ms()
        timeout = self._stream_timeout_ms
        ttl_evict = 3 * timeout

        to_fire: list[tuple[Any, int]] = []
        to_drop_silently: list[Any] = []
        with self._tracker_lock:
            if not self._last_seen_by_stream:
                return
            for stream_key, last_seen in self._last_seen_by_stream.items():
                silence = ts - last_seen
                if silence >= ttl_evict:
                    to_drop_silently.append(stream_key)
                elif silence >= timeout:
                    to_fire.append((stream_key, silence))
            # TTL-sweep entries have no callback; evict in-lock now.
            for stream_key in to_drop_silently:
                self._last_seen_by_stream.pop(stream_key, None)

        for stream_key, silence in to_fire:
            self._logger.info(
                "Stream %r timed out (silence %d ms >= threshold %d ms)",
                stream_key,
                silence,
                timeout,
            )
            try:
                self._on_stream_timeout(stream_key)
            except Exception:
                self._logger.exception(
                    "on_stream_timeout callback raised for %r", stream_key
                )
                # Leave entry in tracker; retry on next check cycle.
                continue
            with self._tracker_lock:
                self._last_seen_by_stream.pop(stream_key, None)

        for stream_key in to_drop_silently:
            self._logger.warning(
                "Stream %r dropped by TTL sweep (silence >= 3x threshold %d ms)",
                stream_key,
                timeout,
            )

    def start(self) -> None:
        """Start the background daemon check thread.

        Idempotent. No-op when :attr:`enabled` is ``False``. Hosts
        call this from ``setup()`` **after** their own resources are
        healthy, so a setup failure tears down cleanly without
        leaving an orphan timer thread running.
        """
        if not self._enabled:
            return
        with self._tracker_lock:
            t = self._timer_thread
            if t is not None and t.is_alive():
                return
            if self._stop.is_set():
                # A prior stop() was called; don't resurrect.
                return
            self._timer_thread = threading.Thread(
                target=self._check_loop,
                daemon=True,
                name=self._thread_name,
            )
            self._timer_thread.start()
        self._logger.info(
            "Started stream-timeout check thread (interval=%d ms, threshold=%d ms)",
            self._check_interval_ms,
            self._stream_timeout_ms,
        )

    def stop(self) -> None:
        """Signal the background thread to exit. Idempotent. No-op when
        :attr:`enabled` is ``False``.
        """
        if not self._enabled:
            return
        self._stop.set()

    # ------------------------------------------------------------------
    # Internals (test-visible)
    # ------------------------------------------------------------------

    def _now_ms(self) -> int:
        """Return the current wall-clock time in milliseconds.

        Tests override this by monkeypatching the instance attribute
        (``tracker._now_ms = lambda: 5000``) to drive deterministic
        timelines without ``time.sleep`` or ``freezegun``.
        """
        return int(time.time() * 1000)

    def _ensure_timer_thread_alive(self) -> None:
        """Start the timer thread if absent or self-terminated.

        MUST be called with ``self._tracker_lock`` held, so the
        is-alive check and the new-thread creation are atomic with
        respect to the loop's own set-to-None under the same lock.
        """
        t = self._timer_thread
        if t is not None and t.is_alive():
            return
        if self._stop.is_set():
            # A prior stop() was called; don't resurrect.
            return
        self._timer_thread = threading.Thread(
            target=self._check_loop,
            daemon=True,
            name=self._thread_name,
        )
        self._timer_thread.start()
        self._logger.info(
            "Stream-timeout thread (re)started by touch() "
            "(interval=%d ms, threshold=%d ms)",
            self._check_interval_ms,
            self._stream_timeout_ms,
        )

    def _check_loop(self) -> None:
        """Periodic timeout-check loop for the background daemon thread.

        Runs :meth:`check_now` every ``self._check_interval_ms``
        milliseconds until either ``self._stop`` is set (:meth:`stop`)
        or the tracker has been empty for ``self._idle_exit_cycles``
        consecutive cycles (self-terminate on sustained idle). A
        raising check must NOT kill the thread (else silent keys
        would go untracked forever), so the body is wrapped in a
        broad ``try/except`` that logs and continues.

        Self-termination policy: if the tracker has been empty for
        ``_idle_exit_cycles`` cycles in a row, the thread clears
        ``self._timer_thread`` under the tracker lock and returns.
        The next :meth:`touch` that records a new last-seen stamp
        respawns it via :meth:`_ensure_timer_thread_alive`. The
        set-to-None + respawn check both live inside the tracker
        lock so there is no window where the tracker has entries
        and no thread is running.
        """
        empty_cycles = 0
        while not self._stop.is_set():
            try:
                self.check_now()
            except Exception:
                self._logger.exception("Periodic stream-timeout check raised")

            with self._tracker_lock:
                if self._last_seen_by_stream:
                    empty_cycles = 0
                else:
                    empty_cycles += 1
                    if empty_cycles >= self._idle_exit_cycles:
                        self._logger.info(
                            "Stream-timeout thread self-terminating "
                            "(tracker empty for %d cycles); will be "
                            "respawned by next touch()",
                            empty_cycles,
                        )
                        self._timer_thread = None
                        return

            self._stop.wait(self._check_interval_ms / 1000)
