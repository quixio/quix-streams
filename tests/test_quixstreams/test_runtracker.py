import pytest

from quixstreams.runtracker import RunCollector, RunTracker


class TestRunTracker:
    @pytest.mark.parametrize(
        "conditions, should_have_stopper",
        [
            ({}, False),
            ({"timeout": -1.0}, False),
            ({"timeout": 0.0}, False),
            ({"timeout": 1.0}, True),
            ({"count": -1}, False),
            ({"count": 0}, False),
            ({"count": 1}, True),
            ({"timeout": 0.0, "count": 0}, False),
            ({"timeout": -1.0, "count": -1}, False),
            ({"timeout": -1.0, "count": 1}, True),
            ({"timeout": 1.0, "count": -1}, True),
            ({"timeout": 1.0, "count": 1}, True),
        ],
    )
    def test_stop_condition_detected(self, caplog, conditions, should_have_stopper):
        run_tracker = RunTracker()
        with caplog.at_level("INFO"):
            run_tracker.set_stop_condition(**conditions)
        condition_detected = "APP STOP CONDITIONS SET" in caplog.text
        assert should_have_stopper is condition_detected


def test_runcollector(message_context_factory):
    collector = RunCollector()

    topic = "topic"
    partition = 0
    offset = 1

    # Add value with all metadata
    collector.add_value_and_metadata(
        value="test",
        key="test",
        timestamp_ms=10,
        headers=[],
        topic=topic,
        partition=partition,
        offset=offset,
    )

    # Add only value
    collector.add_value(value={"field": "value"})

    # Only track the count of the processed outputs without storing them
    collector.increment_count()

    assert collector.count == 3

    items = collector.items
    assert items == [
        {
            "_key": "test",
            "_value": "test",
            "_timestamp": 10,
            "_headers": [],
            "_offset": offset,
            "_topic": topic,
            "_partition": partition,
        },
        {
            "field": "value",
        },
    ]
