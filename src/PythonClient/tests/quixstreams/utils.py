from pandas.testing import assert_frame_equal

import src.quixstreams as qx
from src.quixstreams import TimeseriesData, TimeseriesDataTimestamp, ParameterValueType


def assert_timestamps_equal(ts_a: TimeseriesDataTimestamp,
                            ts_b: TimeseriesDataTimestamp):
    assert ts_a.timestamp_nanoseconds == ts_b.timestamp_nanoseconds, "Timestamp"
    for param_id_a, parameter_value_a in ts_a.parameters.items():
        parameter_value_b = ts_b.parameters.get(param_id_a)
        if parameter_value_b is None and parameter_value_a.value is None:
            # The value was removed from the sent timeseries data at some point, and for performance reasons is just nulled out rather than cleaned up
            continue
        parameter_value_b = ts_b.parameters[param_id_a]
        assert parameter_value_a.type == parameter_value_b.type, "Value type"
        if parameter_value_a.type == ParameterValueType.String:
            assert parameter_value_a.string_value == parameter_value_b.string_value, "Value (string)"
        if parameter_value_a.type == ParameterValueType.Numeric:
            assert parameter_value_a.numeric_value == parameter_value_b.numeric_value, "Value (numeric)"
    for tag_id_a, tag_value_a in ts_a.tags.items():
        tag_value_b = ts_b.tags[tag_id_a]
        assert tag_value_a == tag_value_b, "tag"


def assert_timeseries_data_equal(data_a: TimeseriesData, data_b: TimeseriesData):
    assert len(data_a.timestamps) == len(data_b.timestamps), "Timestamp count"
    for index_a, ts_a in enumerate(data_a.timestamps):
        ts_b = data_b.timestamps[index_a]
        assert_timestamps_equal(ts_a, ts_b)


def assert_dataframes_equal(df1, df2, **kwds):
    """ Assert that two dataframes are equal, ignoring ordering of columns"""

    assert_frame_equal(df1.sort_index(axis=1), df2.sort_index(axis=1),
                       check_names=True, **kwds)


def assert_eventdata_are_equal(data_a: qx.EventData, data_b: qx.EventData):
    assert data_a.id == data_b.id
    assert data_a.timestamp_nanoseconds == data_b.timestamp_nanoseconds
    assert data_a.value == data_b.value
    for tag_id_a, tag_value_a in data_a.tags.items():
        tag_value_b = data_b.tags[tag_id_a]
        assert tag_value_a == tag_value_b
