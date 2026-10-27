import time
import uuid
from threading import Event

import pytest

from quixstreams.models import TopicConfig
from quixstreams.sinks import SinkBackpressureError
from tests.utils import DummySink


@pytest.mark.parametrize("processing_guarantee", ["at-least-once", "exactly-once"])
def test_buffered_app_handles_sink_backpressure_during_revoke(
    app_factory,
    consumer_factory,
    executor,
    monkeypatch,
    random_consumer_group,
    processing_guarantee,
):
    processed = []
    ready = Event()
    revoking = Event()
    revoke_completed = Event()
    finished = Event()
    revoke_aborts = []

    def on_message_processed(topic, partition, offset):
        processed.append((topic, partition, offset))
        if len(processed) == 4:
            ready.set()

    app = app_factory(
        consumer_group=random_consumer_group,
        auto_offset_reset="earliest",
        processing_guarantee=processing_guarantee,
        commit_interval=999,
        consumer_poll_timeout=0.05,
        on_message_processed=on_message_processed,
    )
    topics = [
        app.topic(
            str(uuid.uuid4()),
            config=TopicConfig(num_partitions=2, replication_factor=1),
        )
        for _ in range(2)
    ]

    class RevokeBackpressureSink(DummySink):
        backpressured = False

        def write(self, batch):
            if revoking.is_set() and not self.backpressured:
                self.backpressured = True
                raise SinkBackpressureError(retry_after=0)
            super().write(batch)

    sink = RevokeBackpressureSink()
    app.dataframe(topics[0]).concat(app.dataframe(topics[1])).sink(sink)

    # Observe the real callbacks without replacing buffer cleanup, checkpoint
    # handling, or Kafka's transaction abort.
    original_revoke = app._on_revoke
    original_abort = app._producer.abort_transaction

    def on_revoke(consumer, partitions):
        revoking.set()
        try:
            original_revoke(consumer, partitions)
            if partitions:
                revoke_completed.set()
        finally:
            revoking.clear()

    def abort_transaction(*args, **kwargs):
        result = original_abort(*args, **kwargs)
        if revoking.is_set():
            revoke_aborts.append(True)
        return result

    monkeypatch.setattr(app, "_on_revoke", on_revoke)
    monkeypatch.setattr(app._producer, "abort_transaction", abort_transaction)

    with app.get_producer() as producer:
        for topic in topics:
            for partition in (0, 1):
                producer.produce(
                    topic.name,
                    partition=partition,
                    key=b"key",
                    value=b'{"value": 1}',
                    timestamp=1000,
                )

    def trigger_rebalance():
        try:
            assert ready.wait(15), "The initial records were not processed"
            with consumer_factory(
                consumer_group=random_consumer_group,
                auto_commit_enable=False,
                auto_offset_reset="earliest",
            ) as other:
                other.subscribe([topic.name for topic in topics])
                deadline = time.monotonic() + 15
                while not finished.is_set() and not revoke_completed.is_set():
                    assert time.monotonic() < deadline, "The revoke did not complete"
                    other.poll(0.1)
                app.stop()
        finally:
            app.stop()

    rebalance = executor.submit(trigger_rebalance)
    try:
        app.run(timeout=20)
    finally:
        finished.set()
        rebalance.result(timeout=5)

    assert ready.is_set()
    assert sink.backpressured
    assert revoke_completed.is_set()
    assert bool(revoke_aborts) == (processing_guarantee == "exactly-once")
