import itertools

from quixstreams.sinks import SinkBatch


def test_sink_batch():
    batch = SinkBatch(topic="test", partition=0)
    assert batch.topic == "test"
    assert batch.partition == 0
    assert batch.empty()

    for i in range(10):
        batch.append(value="test", key="test", timestamp=0, headers=[], offset=i)
    assert batch.size == 10
    items = list(batch)
    assert len(items) == 10

    chunks = list(batch.iter_chunks(2))
    assert len(chunks) == 5

    assert list(itertools.chain(*chunks)) == items

    assert batch.start_offset == 0
    assert batch.key_type is str

    batch.clear()
    assert batch.size == 0
    assert len(list(batch)) == 0
    assert len(list(itertools.chain(*batch.iter_chunks(2)))) == 0
