from time import monotonic
from unittest.mock import MagicMock

import pytest
from confluent_kafka import OFFSET_INVALID, Consumer, TopicPartition

from quixstreams.internal_consumer import InternalConsumer
from quixstreams.models import Topic
from tests.utils import ConfluentKafkaMessageStub


def _message(topic, partition, offset, timestamp=None):
    return ConfluentKafkaMessageStub(
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp=(1, offset if timestamp is None else timestamp),
        value=b"{}",
    )


@pytest.fixture
def buffered_consumer(monkeypatch):
    def factory(
        committed_offset=5,
        auto_offset_reset="earliest",
        low_watermark=5,
        high_watermark=5,
        position=OFFSET_INVALID,
        assignment_offset=OFFSET_INVALID,
        max_partition_buffer_size=10000,
        on_assign=None,
    ):
        consumer = InternalConsumer(
            broker_address="localhost:9092",
            consumer_group="buffer-reset-test",
            auto_offset_reset=auto_offset_reset,
            max_partition_buffer_size=max_partition_buffer_size,
        )
        kafka = MagicMock(spec=Consumer)
        monkeypatch.setattr(consumer, "_inner_consumer", kafka)
        assignments = [
            TopicPartition(
                topic, partition, assignment_offset if topic == "quiet" else 0
            )
            for topic in ("quiet", "active")
            for partition in (1, 3)
        ]
        kafka.assignment.return_value = assignments
        kafka.committed.side_effect = lambda partitions, **_: [
            TopicPartition(
                tp.topic, tp.partition, committed_offset if tp.topic == "quiet" else 0
            )
            for tp in partitions
        ]
        kafka.position.side_effect = lambda partitions: [
            TopicPartition(
                tp.topic,
                tp.partition,
                position if tp.topic == "quiet" else OFFSET_INVALID,
            )
            for tp in partitions
        ]
        kafka.get_watermark_offsets.side_effect = lambda partition, **_: (
            (low_watermark, high_watermark) if partition.topic == "quiet" else (0, 3)
        )
        kafka.consume.return_value = []
        consumer.subscribe([Topic("quiet"), Topic("active")], on_assign=on_assign)
        kafka.subscribe.call_args.kwargs["on_assign"](kafka, assignments)
        return consumer, kafka

    return factory


@pytest.mark.parametrize(
    "committed_offset", [4, OFFSET_INVALID], ids=["expired", "missing"]
)
@pytest.mark.parametrize("auto_offset_reset", ["earliest", "latest"])
def test_empty_retained_inputs_release_multiple_paired_partitions(
    buffered_consumer, committed_offset, auto_offset_reset
):
    consumer, kafka = buffered_consumer(
        committed_offset=committed_offset, auto_offset_reset=auto_offset_reset
    )
    kafka.consume.side_effect = [
        [
            _message("active", partition, offset)
            for partition in (1, 3)
            for offset in range(3)
        ],
        *[[] for _ in range(12)],
    ]
    rows = [consumer.poll_row(0, buffered=True) for _ in range(12)]
    assert [(row.partition, row.offset) for row in rows if row is not None] == [
        (partition, offset) for partition in (1, 3) for offset in range(3)
    ]


@pytest.mark.parametrize("auto_offset_reset", ["earliest", "latest"])
def test_retention_during_assignment_preserves_prefetched_records(
    buffered_consumer, auto_offset_reset
):
    def on_assign(kafka, partitions):
        kafka.assign(partitions)
        # Kafka fetches offset 4 before retention advances the floor to 5.
        # position() remains unknown until those fetched records are consumed.
        kafka.get_watermark_offsets.side_effect = lambda partition, **_: (
            (5, 5) if partition.topic == "quiet" else (0, 3)
        )

    consumer, kafka = buffered_consumer(
        committed_offset=4,
        auto_offset_reset=auto_offset_reset,
        low_watermark=0,
        on_assign=on_assign,
    )
    kafka.consume.side_effect = [
        [
            _message(topic, partition, offset, timestamp)
            for partition in (1, 3)
            for topic, offset, timestamp in (("quiet", 4, 1), ("active", 0, 10))
        ],
        *[[] for _ in range(8)],
    ]
    rows = [consumer.poll_row(0, buffered=True) for _ in range(8)]
    assert sorted((row.topic, row.partition, row.offset) for row in rows if row) == [
        (topic, partition, offset)
        for topic, offset in (("active", 0), ("quiet", 4))
        for partition in (1, 3)
    ]
    kafka.seek.assert_not_called()


@pytest.mark.parametrize("committed_offset", [4, OFFSET_INVALID])
def test_latest_nonempty_input_accepts_records_below_queried_high_watermark(
    buffered_consumer, committed_offset
):
    consumer, kafka = buffered_consumer(
        committed_offset=committed_offset, auto_offset_reset="latest", high_watermark=8
    )
    # An unknown live position does not prove the queried high watermark was
    # consumed. A subsequently delivered record must still be accepted and ordered.
    kafka.consume.side_effect = [
        [
            _message(topic, partition, offset, timestamp)
            for partition in (1, 3)
            for topic, offset, timestamp in (("active", 0, 10), ("quiet", 5, 1))
        ],
        [],
    ]
    rows = [consumer.poll_row(0, buffered=True) for _ in range(3)]
    assert [(row.topic, row.partition, row.offset) for row in rows if row] == [
        ("quiet", 1, 5),
        ("quiet", 3, 5),
    ]


@pytest.mark.parametrize("seed_source", ["committed", "position", "assignment"])
def test_latest_preserves_known_start_before_retained_records(
    buffered_consumer, seed_source
):
    kwargs = {"committed_offset": 7, "auto_offset_reset": "latest", "high_watermark": 7}
    if seed_source == "committed":
        kwargs["committed_offset"] = 5
    else:
        kwargs["position" if seed_source == "position" else "assignment_offset"] = 5
    consumer, kafka = buffered_consumer(**kwargs)
    kafka.consume.return_value = [
        _message(topic, partition, offset, timestamp)
        for partition in (1, 3)
        for topic, offset, timestamp in (("active", 0, 10), ("quiet", 5, 1))
    ]
    row = consumer.poll_row(0, buffered=True)
    assert (row.topic, row.partition, row.offset) == ("quiet", 1, 5)


@pytest.mark.parametrize("committed_offset", [4, OFFSET_INVALID])
def test_error_reset_policy_does_not_infer_an_idle_position(
    buffered_consumer, committed_offset
):
    consumer, kafka = buffered_consumer(
        committed_offset=committed_offset, auto_offset_reset="error"
    )
    kafka.consume.return_value = [_message("active", 1, 0)]
    assert consumer.poll_row(0, buffered=True) is None


def test_backpressure_preserves_idle_and_unprocessed_buffers_and_replays_rewound_input(
    buffered_consumer,
):
    consumer, kafka = buffered_consumer()
    kafka.consume.side_effect = [
        [
            _message("active", partition, offset)
            for partition in (1, 3)
            for offset in range(3)
        ],
        [_message("active", 1, offset) for offset in range(3)],
        *[[] for _ in range(12)],
    ]
    first = consumer.poll_row(0, buffered=True)
    assert (first.partition, first.offset) == (1, 0)
    consumer.trigger_backpressure(offsets_to_seek={("active", 1): 0}, resume_after=0)
    consumer.resume_backpressured()
    rows = [consumer.poll_row(0, buffered=True) for _ in range(12)]
    assert sorted((row.partition, row.offset) for row in rows if row) == [
        (partition, offset) for partition in (1, 3) for offset in range(3)
    ]


def test_backpressure_resume_allows_retained_full_buffer_to_pause_again(
    buffered_consumer,
):
    consumer, kafka = buffered_consumer(max_partition_buffer_size=1)
    kafka.consume.side_effect = [
        [_message("active", partition, 0) for partition in (1, 3)],
        [],
    ]
    assert consumer.poll_row(0, buffered=True).partition == 1
    consumer.trigger_backpressure(offsets_to_seek={("active", 1): 0}, resume_after=0)
    consumer.resume_backpressured()
    kafka.pause.reset_mock()
    consumer._feed_buffer(timeout=0)
    kafka.pause.assert_called_once_with([TopicPartition("active", 3)])


def test_backpressure_waits_before_returning_retained_rows(
    buffered_consumer, monkeypatch
):
    now = [0.0]
    monkeypatch.setattr(
        "quixstreams.internal_consumer.consumer.monotonic", lambda: now[0]
    )
    consumer, kafka = buffered_consumer()
    kafka.consume.side_effect = [
        [
            _message("active", partition, offset)
            for partition in (1, 3)
            for offset in range(3)
        ],
        [],
        [],
        [_message("active", 1, offset) for offset in range(3)],
        *[[] for _ in range(12)],
    ]
    assert consumer.poll_row(0, buffered=True).partition == 1
    consumer.trigger_backpressure(offsets_to_seek={("active", 1): 0}, resume_after=5)
    for timestamp in (0, 4):
        now[0] = timestamp
        consumer.resume_backpressured()
        assert consumer.poll_row(0, buffered=True) is None
    # Poll Kafka while paused so rebalances and other callbacks still run.
    assert kafka.consume.call_count == 3

    now[0] = 5
    consumer.resume_backpressured()
    rows = [consumer.poll_row(0, buffered=True) for _ in range(12)]
    assert sorted((row.partition, row.offset) for row in rows if row) == [
        (partition, offset) for partition in (1, 3) for offset in range(3)
    ]


@pytest.mark.parametrize(
    "committed_offset", [4, OFFSET_INVALID], ids=["expired", "missing"]
)
@pytest.mark.parametrize("auto_offset_reset", ["earliest", "latest"])
def test_empty_retained_inputs_with_kafka(
    consumer_factory,
    internal_consumer_factory,
    topic_json_serdes_factory,
    producer,
    kafka_admin_client,
    committed_offset,
    auto_offset_reset,
):
    quiet = topic_json_serdes_factory(num_partitions=4)
    active = topic_json_serdes_factory(num_partitions=4)
    with producer:
        for partition in (1, 3):
            for offset in range(5):
                producer.produce(quiet.name, partition=partition, value=b"{}")
            for offset in range(3):
                producer.produce(active.name, partition=partition, value=b"{}")
    with consumer_factory(auto_commit_enable=False) as probe:
        offsets = [TopicPartition(active.name, partition, 0) for partition in (1, 3)]
        if committed_offset >= 0:
            offsets += [
                TopicPartition(quiet.name, partition, committed_offset)
                for partition in (1, 3)
            ]
        probe.commit(offsets=offsets, asynchronous=False)
        for future in kafka_admin_client.delete_records(
            [TopicPartition(quiet.name, partition, 5) for partition in (1, 3)]
        ).values():
            future.result(timeout=15)
        for partition in (1, 3):
            assert probe.get_watermark_offsets(
                TopicPartition(quiet.name, partition), timeout=5
            ) == (5, 5)

    rows = []
    with internal_consumer_factory(auto_offset_reset=auto_offset_reset) as consumer:
        consumer.subscribe([quiet, active])
        deadline = monotonic() + 10
        while len(rows) < 6 and monotonic() < deadline:
            row = consumer.poll_row(0.1, buffered=True)
            if row is not None:
                rows.append(row)
    assert sorted((row.topic, row.partition, row.offset) for row in rows) == [
        (active.name, partition, offset) for partition in (1, 3) for offset in range(3)
    ]


def test_backpressure_preserves_idle_inputs_with_kafka(
    consumer_factory, internal_consumer_factory, topic_json_serdes_factory, producer
):
    quiet = topic_json_serdes_factory(num_partitions=4)
    active = topic_json_serdes_factory(num_partitions=4)
    with producer:
        for partition in (1, 3):
            for offset in range(3):
                producer.produce(quiet.name, partition=partition, value=b"{}")
                producer.produce(active.name, partition=partition, value=b"{}")
    with consumer_factory(auto_commit_enable=False) as probe:
        probe.commit(
            offsets=[
                TopicPartition(topic.name, partition, offset)
                for topic, offset in ((quiet, 3), (active, 0))
                for partition in (1, 3)
            ],
            asynchronous=False,
        )

    with internal_consumer_factory(auto_offset_reset="earliest") as consumer:
        consumer.subscribe([quiet, active])
        first = None
        deadline = monotonic() + 10
        while first is None and monotonic() < deadline:
            first = consumer.poll_row(0.1, buffered=True)
        assert first is not None
        assert (first.topic, first.offset) == (active.name, 0)
        consumer.trigger_backpressure(
            offsets_to_seek={(first.topic, first.partition): first.offset},
            resume_after=0,
        )
        consumer.resume_backpressured()
        rows = []
        deadline = monotonic() + 10
        while len(rows) < 6 and monotonic() < deadline:
            row = consumer.poll_row(0.1, buffered=True)
            if row is not None:
                rows.append(row)
    assert sorted((row.topic, row.partition, row.offset) for row in rows) == [
        (active.name, partition, offset) for partition in (1, 3) for offset in range(3)
    ]
