from unittest import mock

import pytest

from quixstreams.dataframe import DataFrameRegistry
from quixstreams.models import Topic


class TestDataFrameRegistry:
    def test_register_root_multi_topic_sdf_fails(self):
        registry = DataFrameRegistry()
        sdf_mock = mock.Mock(topics=[Topic("test-1"), Topic("test-1")])
        with pytest.raises(
            ValueError, match="Expected a StreamingDataFrame with one topic"
        ):
            registry.register_root(sdf_mock)

    def test_register_state_id_success(self):
        registry = DataFrameRegistry()
        registry.register_state_id(state_id="id", topic_names=["topic1", "topic2"])

        assert sorted(registry.get_topics_for_state_id("id")) == sorted(
            ["topic1", "topic2"]
        )
        assert registry.get_state_ids("topic1") == ["id"]
        assert registry.get_state_ids("topic2") == ["id"]
