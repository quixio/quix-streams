import datetime
from typing import Iterable, Optional
from unittest.mock import MagicMock

import influxdb_client_3
import pytest
from influxdb_client_3 import InfluxDBClient3

from quixstreams.sinks import SinkBackpressureError
from quixstreams.sinks.core.influxdb3 import InfluxDB3Sink, TimePrecision


@pytest.fixture()
def influxdb3_sink_factory():
    def factory(
        client_mock: MagicMock,
        measurement: str,
        fields_keys: Iterable[str] = (),
        tags_keys: Iterable[str] = (),
        time_key: Optional[str] = None,
        batch_size: int = 1000,
        time_precision: TimePrecision = "ms",
        convert_ints_to_floats: bool = False,
        include_metadata_tags: bool = False,
    ) -> InfluxDB3Sink:
        sink = InfluxDB3Sink(
            token="",
            host="",
            organization_id="test",
            database="test",
            measurement=measurement,
            fields_keys=fields_keys,
            tags_keys=tags_keys,
            time_key=time_key,
            time_precision=time_precision,
            include_metadata_tags=include_metadata_tags,
            convert_ints_to_floats=convert_ints_to_floats,
            batch_size=batch_size,
        )
        sink._client = client_mock
        return sink

    return factory


class TestInfluxDB3Sink:
    def test_write_success(self, influxdb3_sink_factory):
        client_mock = MagicMock(spec_set=InfluxDBClient3)
        measurement = "measurement"
        sink = influxdb3_sink_factory(client_mock=client_mock, measurement=measurement)
        topic = "test-topic"

        value, timestamp = {"key": "value"}, 1
        for partition in (0, 1):
            sink.add(
                value=value,
                key="key",
                timestamp=timestamp,
                headers=[],
                topic=topic,
                partition=partition,
                offset=1,
            )
        sink.flush()

        assert client_mock.write.call_count == 2
        first_call = client_mock.write.call_args_list[0]
        assert first_call.kwargs == {
            "record": [
                {
                    "measurement": measurement,
                    "tags": {},
                    "fields": value,
                    "time": timestamp,
                }
            ],
            "write_precision": "ms",
        }

    def test_write_fields_keys(self, influxdb3_sink_factory):
        client_mock = MagicMock(spec_set=InfluxDBClient3)
        measurement = "measurement"
        fields_keys = ["key1"]

        sink = influxdb3_sink_factory(
            client_mock=client_mock, measurement=measurement, fields_keys=fields_keys
        )
        topic = "test-topic"

        value, timestamp = {"key1": 1, "key2": 2}, 1
        sink.add(
            value=value,
            key="key",
            timestamp=timestamp,
            headers=[],
            topic=topic,
            partition=0,
            offset=1,
        )
        sink.flush()

        client_mock.write.assert_called_once_with(
            record=[
                {
                    "measurement": measurement,
                    "tags": {},
                    "fields": {"key1": 1},
                    "time": timestamp,
                }
            ],
            write_precision="ms",
        )

    def test_write_tags_keys(self, influxdb3_sink_factory):
        client_mock = MagicMock(spec_set=InfluxDBClient3)
        measurement = "measurement"
        tags_keys = ["tag1"]

        sink = influxdb3_sink_factory(
            client_mock=client_mock, measurement=measurement, tags_keys=tags_keys
        )
        topic = "test-topic"

        value, timestamp = {"key1": 1, "tag1": 1}, 1
        sink.add(
            value=value,
            key="key",
            timestamp=timestamp,
            headers=[],
            topic=topic,
            partition=0,
            offset=1,
        )
        sink.flush()

        client_mock.write.assert_called_once_with(
            record=[
                {
                    "measurement": measurement,
                    "tags": {"tag1": 1},
                    "fields": value,
                    "time": timestamp,
                }
            ],
            write_precision="ms",
        )

    def test_write_values_not_dicts_fail(self, influxdb3_sink_factory):
        client_mock = MagicMock(spec_set=InfluxDBClient3)
        measurement = "measurement"

        sink = influxdb3_sink_factory(client_mock=client_mock, measurement=measurement)
        topic = "test-topic"

        value, timestamp = 1, 1
        with pytest.raises(TypeError, match="supports only dictionaries"):
            sink.add(
                value=value,
                key="key",
                timestamp=timestamp,
                headers=[],
                topic=topic,
                partition=0,
                offset=1,
            )

    def test_write_tags_keys_excluded_from_fields(self, influxdb3_sink_factory):
        client_mock = MagicMock(spec_set=InfluxDBClient3)
        measurement = "measurement"

        sink = influxdb3_sink_factory(
            client_mock=client_mock, measurement=measurement, tags_keys=["b"]
        )
        topic = "test-topic"

        value, timestamp = {"a": 1, "b": 2}, 1
        sink.add(
            value=value,
            key="key",
            timestamp=timestamp,
            headers=[],
            topic=topic,
            partition=0,
            offset=1,
        )
        sink.flush()

        client_mock.write.assert_called_once_with(
            record=[
                {
                    "measurement": measurement,
                    "tags": {"b": 2},
                    "fields": {"a": 1},
                    "time": timestamp,
                }
            ],
            write_precision="ms",
        )

    def test_init_fields_keys_and_tags_keys_overlap_fails(self, influxdb3_sink_factory):
        client_mock = MagicMock(spec_set=InfluxDBClient3)
        measurement = "measurement"

        with pytest.raises(
            ValueError, match='are present in both "fields_keys" and "tags_keys"'
        ):
            influxdb3_sink_factory(
                client_mock=client_mock,
                measurement=measurement,
                tags_keys=["b"],
                fields_keys=["b"],
            )

    def test_write_include_metadata_tags_true(self, influxdb3_sink_factory):
        client_mock = MagicMock(spec_set=InfluxDBClient3)
        measurement = "measurement"

        sink = influxdb3_sink_factory(
            client_mock=client_mock, measurement=measurement, include_metadata_tags=True
        )
        topic = "test-topic"

        key, value, timestamp = "key", {"key1": 1, "tag1": 1}, 1
        sink.add(
            value=value,
            key=key,
            timestamp=timestamp,
            headers=[],
            topic=topic,
            partition=0,
            offset=1,
        )
        sink.flush()

        client_mock.write.assert_called_once_with(
            record=[
                {
                    "measurement": measurement,
                    "tags": {"__topic": topic, "__partition": 0, "__key": key},
                    "fields": value,
                    "time": timestamp,
                }
            ],
            write_precision="ms",
        )

    def test_write_batch_size(self, influxdb3_sink_factory):
        client_mock = MagicMock(spec_set=InfluxDBClient3)
        measurement = "measurement"

        sink = influxdb3_sink_factory(
            client_mock=client_mock, measurement=measurement, batch_size=1
        )
        topic = "test-topic"

        value1, value2 = {"key": "value1"}, {"key": "value2"}
        timestamp = 1
        sink.add(
            value=value1,
            key="key",
            timestamp=timestamp,
            headers=[],
            topic=topic,
            partition=0,
            offset=1,
        )
        sink.add(
            value=value2,
            key="key",
            timestamp=timestamp,
            headers=[],
            topic=topic,
            partition=0,
            offset=2,
        )
        sink.flush()

        assert client_mock.write.call_count == 2
        first_call, second_call = client_mock.write.call_args_list
        assert first_call.kwargs == {
            "record": [
                {
                    "measurement": measurement,
                    "tags": {},
                    "fields": value1,
                    "time": timestamp,
                }
            ],
            "write_precision": "ms",
        }
        assert second_call.kwargs == {
            "record": [
                {
                    "measurement": measurement,
                    "tags": {},
                    "fields": value2,
                    "time": timestamp,
                }
            ],
            "write_precision": "ms",
        }

    def test_write_backpressured(self, influxdb3_sink_factory):
        class Response:
            status = 429

        influx_429_err = influxdb_client_3.InfluxDBError()
        influx_429_err.retry_after = 10
        influx_429_err.response = Response()

        client_mock = MagicMock(spec_set=InfluxDBClient3)
        client_mock.write.side_effect = influx_429_err
        measurement = "measurement"

        sink = influxdb3_sink_factory(
            client_mock=client_mock, measurement=measurement, batch_size=1
        )
        topic = "test-topic"

        value1 = {"key": "value1"}
        timestamp = 1
        sink.add(
            value=value1,
            key="key",
            timestamp=timestamp,
            headers=[],
            topic=topic,
            partition=0,
            offset=1,
        )
        with pytest.raises(SinkBackpressureError) as raised:
            sink.flush()

        assert raised.value.retry_after == 10

    def test_write_fails_propagates_exception(self, influxdb3_sink_factory):
        influx_some_err = influxdb_client_3.InfluxDBError()

        client_mock = MagicMock(spec_set=InfluxDBClient3)
        client_mock.write.side_effect = influx_some_err
        measurement = "measurement"

        sink = influxdb3_sink_factory(
            client_mock=client_mock, measurement=measurement, batch_size=1
        )
        topic = "test-topic"

        value1 = {"key": "value1"}
        timestamp = 1
        sink.add(
            value=value1,
            key="key",
            timestamp=timestamp,
            headers=[],
            topic=topic,
            partition=0,
            offset=1,
        )
        with pytest.raises(influxdb_client_3.InfluxDBError):
            sink.flush()

    @pytest.mark.parametrize(
        "fields_keys, result",
        [
            ((), {"str_key": "value", "int_key": 0.0, "float_key": 1.1}),
            (("str_key", "int_key"), {"str_key": "value", "int_key": 0.0}),
        ],
    )
    def test_convert_ints_to_floats(self, influxdb3_sink_factory, fields_keys, result):
        client_mock = MagicMock(spec_set=InfluxDBClient3)
        measurement = "measurement"
        sink = influxdb3_sink_factory(
            client_mock=client_mock,
            measurement=measurement,
            fields_keys=fields_keys,
            convert_ints_to_floats=True,
        )
        topic = "test-topic"

        value, timestamp = {"str_key": "value", "int_key": 0, "float_key": 1.1}, 1
        sink.add(
            value=value,
            key="key",
            timestamp=timestamp,
            headers=[],
            topic=topic,
            partition=0,
            offset=1,
        )
        sink.flush()

        client_mock.write.assert_called_once_with(
            record=[
                {
                    "measurement": measurement,
                    "tags": {},
                    "fields": result,
                    "time": timestamp,
                }
            ],
            write_precision="ms",
        )

    @pytest.mark.parametrize(
        "time",
        (
            1625140800123,
            "2021-07-01T00:00:00Z",
            datetime.datetime(2021, 7, 1, 0, 0, 0, 123456),
        ),
    )
    def test_valid_timestamps(self, influxdb3_sink_factory, time, caplog):
        """
        Valid timestamps are accepted and correctly recognize as mins/maxes.
        """
        client_mock = MagicMock(spec_set=InfluxDBClient3)
        measurement = "measurement"
        sink = influxdb3_sink_factory(
            client_mock=client_mock,
            measurement=measurement,
            convert_ints_to_floats=True,
            time_key="time",
        )
        topic = "test-topic"

        value = {"str_key": "value", "int_key": 10, "time": time}
        sink.add(
            value=value,
            key="key",
            timestamp=1,
            headers=[],
            topic=topic,
            partition=0,
            offset=1,
        )
        with caplog.at_level("INFO"):
            sink.flush()
            assert f"min_timestamp={str(time)}" in caplog.text
            assert f"max_timestamp={str(time)}" in caplog.text

        client_mock.write.assert_called_once_with(
            record=[
                {
                    "measurement": measurement,
                    "tags": {},
                    "fields": value,
                    "time": time,
                }
            ],
            write_precision="ms",
        )

    def test_invalid_int_timestamp(self, influxdb3_sink_factory):
        """
        Integer timestamps must match the precision length else raise an error.
        """
        precision = "ms"
        client_mock = MagicMock(spec_set=InfluxDBClient3)
        measurement = "measurement"
        sink = influxdb3_sink_factory(
            client_mock=client_mock,
            measurement=measurement,
            convert_ints_to_floats=True,
            time_key="time",
            time_precision=precision,
        )
        topic = "test-topic"

        value = {"str_key": "value", "int_key": 10, "time": 1234567890123456}
        sink.add(
            value=value,
            key="key",
            timestamp=1,
            headers=[],
            topic=topic,
            partition=0,
            offset=1,
        )
        with pytest.raises(ValueError) as e:
            sink.flush()

        error_str = str(e)
        assert precision in error_str
        assert "got 16" in error_str
