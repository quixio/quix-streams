from functools import partial

import pytest

from quixstreams.models.topics.exceptions import TopicPartitionsMismatch
from quixstreams.state.exceptions import StoreAlreadyRegisteredError


class TestStreamingDataFrameJoinAsOf:
    @pytest.fixture
    def create_sdf(self, dataframe_factory, state_manager):
        def _create_sdf(topic):
            return dataframe_factory(topic=topic, state_manager=state_manager)

        return _create_sdf

    @pytest.fixture
    def assign_partition(self, state_manager):
        def _assign_partition(sdf):
            state_manager.on_partition_assign(
                stream_id=sdf.stream_id,
                partition=0,
                committed_offsets={},
            )

        return _assign_partition

    @pytest.fixture
    def publish(self, message_context_factory):
        def _publish(sdf, topic, value, key, timestamp):
            return sdf.test(
                value=value,
                key=key,
                timestamp=timestamp,
                topic=topic,
                ctx=message_context_factory(topic=topic.name),
            )

        return _publish

    @pytest.mark.parametrize(
        "how, right, left, expected",
        [
            (
                "inner",
                {"right": 2},
                {"left": 1},
                [({"left": 1, "right": 2}, b"key", 2, None)],
            ),
            (
                "inner",
                None,
                {"left": 1},
                [],
            ),
            (
                "inner",
                {},
                {"left": 1},
                [],
            ),
            (
                "left",
                {"right": 2},
                {"left": 1},
                [({"left": 1, "right": 2}, b"key", 2, None)],
            ),
            (
                "left",
                None,
                {"left": 1},
                [({"left": 1}, b"key", 2, None)],
            ),
            (
                "left",
                {},
                {"left": 1},
                [({"left": 1}, b"key", 2, None)],
            ),
        ],
    )
    def test_how(
        self,
        topic_manager_topic_factory,
        create_sdf,
        assign_partition,
        publish,
        how,
        right,
        left,
        expected,
    ):
        left_topic = topic_manager_topic_factory()
        right_topic = topic_manager_topic_factory()
        left_sdf, right_sdf = create_sdf(left_topic), create_sdf(right_topic)
        joined_sdf = left_sdf.join_asof(right_sdf, how=how)
        assign_partition(right_sdf)

        publish(joined_sdf, right_topic, value=right, key=b"key", timestamp=1)
        joined_value = publish(
            joined_sdf, left_topic, value=left, key=b"key", timestamp=2
        )
        assert joined_value == expected

    def test_how_invalid_value(self, topic_manager_topic_factory, create_sdf):
        left_topic = topic_manager_topic_factory()
        right_topic = topic_manager_topic_factory()
        left_sdf, right_sdf = create_sdf(left_topic), create_sdf(right_topic)

        match = 'Invalid "how" value'
        with pytest.raises(ValueError, match=match):
            left_sdf.join_asof(right_sdf, how="invalid")

    def test_mismatching_partitions_fails(
        self, topic_manager_topic_factory, create_sdf
    ):
        left_topic = topic_manager_topic_factory()
        right_topic = topic_manager_topic_factory(partitions=2)
        left_sdf, right_sdf = create_sdf(left_topic), create_sdf(right_topic)

        with pytest.raises(TopicPartitionsMismatch):
            left_sdf.join_asof(right_sdf)

    @pytest.mark.parametrize(
        "on_merge, right, left, expected",
        [
            (
                "keep-left",
                None,
                {"A": 1},
                {"A": 1},
            ),
            (
                "keep-left",
                {"B": "right", "C": 2},
                {"A": 1, "B": "left"},
                {"A": 1, "B": "left", "C": 2},
            ),
            (
                "keep-right",
                None,
                {"A": 1},
                {"A": 1},
            ),
            (
                "keep-right",
                {"B": "right", "C": 2},
                {"A": 1, "B": "left"},
                {"A": 1, "B": "right", "C": 2},
            ),
            (
                "raise",
                None,
                {"A": 1},
                {"A": 1},
            ),
            (
                "raise",
                {"B": 2},
                {"A": 1},
                {"A": 1, "B": 2},
            ),
            (
                "raise",
                {"B": "right B", "C": "right C"},
                {"A": 1, "B": "left B", "C": "left C"},
                ValueError("Overlapping columns: B, C."),
            ),
        ],
    )
    def test_on_merge(
        self,
        topic_manager_topic_factory,
        create_sdf,
        assign_partition,
        publish,
        on_merge,
        right,
        left,
        expected,
    ):
        left_topic = topic_manager_topic_factory()
        right_topic = topic_manager_topic_factory()
        left_sdf, right_sdf = create_sdf(left_topic), create_sdf(right_topic)
        joined_sdf = left_sdf.join_asof(right_sdf, how="left", on_merge=on_merge)
        assign_partition(right_sdf)

        publish(joined_sdf, right_topic, value=right, key=b"key", timestamp=1)

        if isinstance(expected, Exception):
            with pytest.raises(expected.__class__, match=expected.args[0]):
                publish(joined_sdf, left_topic, value=left, key=b"key", timestamp=2)
        else:
            joined_value = publish(
                joined_sdf, left_topic, value=left, key=b"key", timestamp=2
            )
            assert joined_value == [(expected, b"key", 2, None)]

    def test_on_merge_invalid_value(self, topic_manager_topic_factory, create_sdf):
        left_topic = topic_manager_topic_factory()
        right_topic = topic_manager_topic_factory()
        left_sdf, right_sdf = create_sdf(left_topic), create_sdf(right_topic)

        match = 'Invalid "on_merge"'
        with pytest.raises(ValueError, match=match):
            left_sdf.join_asof(right_sdf, on_merge="invalid")

    def test_on_merge_callback(
        self, topic_manager_topic_factory, create_sdf, assign_partition, publish
    ):
        left_topic = topic_manager_topic_factory()
        right_topic = topic_manager_topic_factory()
        left_sdf, right_sdf = create_sdf(left_topic), create_sdf(right_topic)

        def on_merge(left, right):
            return {"left": left, "right": right}

        joined_sdf = left_sdf.join_asof(right_sdf, on_merge=on_merge)
        assign_partition(right_sdf)

        publish(joined_sdf, right_topic, value=1, key=b"key", timestamp=1)
        joined_value = publish(joined_sdf, left_topic, value=2, key=b"key", timestamp=2)
        assert joined_value == [({"left": 2, "right": 1}, b"key", 2, None)]

    def test_grace_ms(
        self,
        topic_manager_topic_factory,
        create_sdf,
        assign_partition,
        publish,
    ):
        left_topic = topic_manager_topic_factory()
        right_topic = topic_manager_topic_factory()
        left_sdf, right_sdf = create_sdf(left_topic), create_sdf(right_topic)

        joined_sdf = left_sdf.join_asof(right_sdf, grace_ms=10)
        assign_partition(right_sdf)

        # min eligible timestamp is 15 - 10 = 5
        publish(joined_sdf, right_topic, value={"right": 1}, key=b"key", timestamp=15)

        # min eligible timestamp is still 5
        publish(joined_sdf, right_topic, value={"right": 3}, key=b"key", timestamp=4)
        publish(joined_sdf, right_topic, value={"right": 2}, key=b"key", timestamp=5)

        publish_left = partial(
            publish,
            joined_sdf,
            left_topic,
            value={"left": 4},
            key=b"key",
        )

        assert publish_left(timestamp=4) == []
        assert publish_left(timestamp=5) == [({"left": 4, "right": 2}, b"key", 5, None)]

    def test_self_join_not_supported(self, topic_manager_topic_factory, create_sdf):
        topic = topic_manager_topic_factory()
        match = (
            "Joining dataframes originating from the same topic is not yet supported."
        )

        # The very same sdf object
        sdf = create_sdf(topic)
        with pytest.raises(ValueError, match=match):
            sdf.join_asof(sdf)

        # Same topic, different branch
        sdf2 = sdf.apply(lambda v: v)
        with pytest.raises(ValueError, match=match):
            sdf.join_asof(sdf2)

    def test_join_same_topic_multiple_times_fails(
        self, topic_manager_topic_factory, create_sdf
    ):
        topic1 = topic_manager_topic_factory()
        topic2 = topic_manager_topic_factory()
        topic3 = topic_manager_topic_factory()

        sdf1 = create_sdf(topic1)
        sdf2 = create_sdf(topic2)
        sdf3 = create_sdf(topic3)

        # Join topic1 with topic2 once
        sdf1.join_asof(sdf2)

        # Repeat the join
        with pytest.raises(StoreAlreadyRegisteredError):
            sdf1.join_asof(sdf2)

        # Try joining topic2 with another sdf
        with pytest.raises(StoreAlreadyRegisteredError):
            sdf3.join_asof(sdf2)
