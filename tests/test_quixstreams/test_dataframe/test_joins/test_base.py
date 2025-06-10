import pytest

from quixstreams.dataframe.joins.base import Join
from quixstreams.models.topics.exceptions import TopicPartitionsMismatch


def test_how_invalid_value():
    with pytest.raises(ValueError, match='Invalid "how" value'):
        Join(how="invalid", on_merge="raise", grace_ms=1)


def test_on_merge_invalid_value():
    with pytest.raises(ValueError, match='Invalid "on_merge"'):
        Join(how="inner", on_merge="invalid", grace_ms=1)


def test_mismatching_partitions_fails(topic_manager_topic_factory, create_sdf):
    left_topic = topic_manager_topic_factory()
    right_topic = topic_manager_topic_factory(partitions=2)
    left_sdf, right_sdf = create_sdf(left_topic), create_sdf(right_topic)
    join = Join(how="inner", on_merge="raise", grace_ms=1)

    with pytest.raises(TopicPartitionsMismatch):
        join.join(left_sdf, right_sdf)


def test_self_join_not_supported(topic_manager_topic_factory, create_sdf):
    match = "Joining dataframes originating from the same topic is not yet supported."
    join = Join(how="inner", on_merge="raise", grace_ms=1)

    # The very same sdf object
    sdf = create_sdf(topic_manager_topic_factory())
    with pytest.raises(ValueError, match=match):
        join.join(sdf, sdf)

    # Same topic, different branch
    sdf2 = sdf.apply(lambda _: _)
    with pytest.raises(ValueError, match=match):
        join.join(sdf, sdf2)
