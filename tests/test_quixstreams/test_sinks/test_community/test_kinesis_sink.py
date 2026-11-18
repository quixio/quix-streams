from unittest.mock import MagicMock

import pytest

from quixstreams.sinks.base import SinkBackpressureError
from quixstreams.sinks.community.kinesis import KinesisSink


@pytest.fixture
def sink():
    instance = KinesisSink(stream_name="test-stream")
    instance._client = MagicMock()
    instance._client.put_records.return_value = {"FailedRecordCount": 0, "Records": []}
    try:
        yield instance
    finally:
        instance._executor.shutdown(wait=True)


def add_records(sink, count=1, partition=0):
    for offset in range(count):
        sink.add(
            value={"offset": offset},
            key=b"key",
            timestamp=0,
            headers=[],
            topic="test-topic",
            partition=partition,
            offset=offset,
        )


def test_flush_does_not_resend_successful_records(sink):
    add_records(sink, count=2)
    sink.flush()
    sink.flush()
    sink._client.put_records.assert_called_once()
    assert len(sink._client.put_records.call_args.kwargs["Records"]) == 2


@pytest.mark.parametrize("eager", [False, True])
@pytest.mark.parametrize("failure", ["partial", "exception"])
def test_flush_rejects_failed_delivery_and_recovers(sink, eager, failure):
    count = 500 if eager else 2
    if failure == "partial":
        sink._client.put_records.return_value = {
            "FailedRecordCount": 1,
            "Records": [{"ErrorCode": "ProvisionedThroughputExceededException"}],
        }
    else:
        sink._client.put_records.side_effect = RuntimeError("request failed")
    add_records(sink, count=count)
    with pytest.raises(SinkBackpressureError):
        sink.flush()

    # Checkpoint replay supplies the batch again; a failed request must not poison it.
    sink._client.put_records.side_effect = None
    sink._client.put_records.return_value = {"FailedRecordCount": 0, "Records": []}
    add_records(sink, count=count)
    sink.flush()
    sink.flush()
    assert sink._client.put_records.call_count == 2
    assert not sink._records
    assert not sink._futures


@pytest.mark.parametrize("count", [0, 1, 499, 500, 501, 1000])
def test_flush_delivers_each_record_once_across_partitions(sink, count):
    add_records(sink, count=count)
    add_records(sink, count=1, partition=1)
    sink.flush()
    sink.flush()
    requests = sink._client.put_records.call_args_list
    assert sum(len(call.kwargs["Records"]) for call in requests) == count + 1
    assert all(len(call.kwargs["Records"]) <= 500 for call in requests)
    assert all(call.kwargs["StreamName"] == "test-stream" for call in requests)
    assert not sink._records
    assert not sink._futures
