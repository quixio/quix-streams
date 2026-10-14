"""Unit tests for the sample iterators in ``sample_iter.py``: per-sample
timestamps, parameter names and signal filtering."""

from quixstreams.models.serializers.ma_streaming_open_data import open_data_pb2 as pb
from quixstreams.models.serializers.ma_streaming_open_data.sample_iter import (
    iter_periodic_proto,
    iter_row_proto,
    iter_synchro_proto,
)

from .builders import (
    double_column,
    double_row,
    empty_column,
    empty_row,
    periodic_data_packet,
    row_data_packet,
    synchro_data_packet,
)

VALID = pb.DataStatus.DATA_STATUS_VALID
MISSING = pb.DataStatus.DATA_STATUS_MISSING


class TestIterPeriodicProto:
    def test_reconstructs_timestamp_as_start_plus_i_times_interval(self):
        """PeriodicData timestamps are start_time + i * interval."""
        names = ["vCar"]
        pkt = periodic_data_packet(
            names=names,
            start_time=1000,
            interval=10,
            columns=[double_column([(1.0, VALID), (2.0, VALID), (3.0, VALID)])],
        )
        rows = list(iter_periodic_proto(pkt, names))
        assert rows == [
            ("vCar", 1000, 1.0, VALID),
            ("vCar", 1010, 2.0, VALID),
            ("vCar", 1020, 3.0, VALID),
        ]

    def test_multiple_columns_no_signal_filter(self):
        names = ["vCar", "nEngine"]
        pkt = periodic_data_packet(
            names=names,
            start_time=0,
            interval=1,
            columns=[
                double_column([(1.0, VALID)]),
                double_column([(2.0, MISSING)]),
            ],
        )
        rows = list(iter_periodic_proto(pkt, names))
        assert rows == [
            ("vCar", 0, 1.0, VALID),
            ("nEngine", 0, 2.0, MISSING),
        ]

    def test_signal_filter_keeps_only_allowlisted_columns(self):
        names = ["vCar", "nEngine"]
        pkt = periodic_data_packet(
            names=names,
            start_time=0,
            interval=1,
            columns=[
                double_column([(1.0, VALID)]),
                double_column([(2.0, VALID)]),
            ],
        )
        rows = list(iter_periodic_proto(pkt, names, signals=frozenset({"nEngine"})))
        assert rows == [("nEngine", 0, 2.0, VALID)]

    def test_signal_filter_matching_nothing_yields_no_rows(self):
        names = ["vCar"]
        pkt = periodic_data_packet(
            names=names,
            start_time=0,
            interval=1,
            columns=[double_column([(1.0, VALID)])],
        )
        rows = list(iter_periodic_proto(pkt, names, signals=frozenset({"missing"})))
        assert rows == []

    def test_column_with_unset_oneof_yields_no_samples(self):
        names = ["vCar"]
        pkt = periodic_data_packet(
            names=names,
            start_time=0,
            interval=1,
            columns=[empty_column()],
        )
        assert list(iter_periodic_proto(pkt, names)) == []


class TestIterSynchroProto:
    def test_reconstructs_cumulative_timestamps(self):
        """SynchroData timestamps are start_time plus the cumulative intervals."""
        names = ["vCar"]
        pkt = synchro_data_packet(
            names=names,
            start_time=1000,
            intervals=[5, 7],
            columns=[double_column([(1.0, VALID), (2.0, VALID), (3.0, VALID)])],
        )
        rows = list(iter_synchro_proto(pkt, names))
        assert rows == [
            ("vCar", 1000, 1.0, VALID),
            ("vCar", 1005, 2.0, VALID),
            ("vCar", 1012, 3.0, VALID),
        ]

    def test_signal_filter_keeps_only_allowlisted_columns(self):
        names = ["vCar", "nEngine"]
        pkt = synchro_data_packet(
            names=names,
            start_time=0,
            intervals=[1],
            columns=[
                double_column([(1.0, VALID)]),
                double_column([(2.0, VALID)]),
            ],
        )
        rows = list(iter_synchro_proto(pkt, names, signals=frozenset({"vCar"})))
        assert rows == [("vCar", 0, 1.0, VALID)]


class TestIterRowProto:
    def test_reconstructs_per_row_timestamps(self):
        """RowData samples take the timestamp of their row."""
        names = ["vCar", "nEngine"]
        pkt = row_data_packet(
            names=names,
            timestamps=[100, 200],
            rows=[
                double_row([(1.0, VALID), (10.0, VALID)]),
                double_row([(2.0, VALID), (20.0, VALID)]),
            ],
        )
        rows = list(iter_row_proto(pkt, names))
        assert rows == [
            ("vCar", 100, 1.0, VALID),
            ("nEngine", 100, 10.0, VALID),
            ("vCar", 200, 2.0, VALID),
            ("nEngine", 200, 20.0, VALID),
        ]

    def test_signal_filter_keeps_only_allowlisted_columns_per_row(self):
        names = ["vCar", "nEngine"]
        pkt = row_data_packet(
            names=names,
            timestamps=[100],
            rows=[double_row([(1.0, VALID), (10.0, VALID)])],
        )
        rows = list(iter_row_proto(pkt, names, signals=frozenset({"nEngine"})))
        assert rows == [("nEngine", 100, 10.0, VALID)]

    def test_signal_filter_matching_nothing_yields_no_rows(self):
        names = ["vCar"]
        pkt = row_data_packet(
            names=names,
            timestamps=[100],
            rows=[double_row([(1.0, VALID)])],
        )
        assert list(iter_row_proto(pkt, names, signals=frozenset({"missing"}))) == []

    def test_row_with_unset_oneof_is_skipped(self):
        names = ["vCar"]
        pkt = row_data_packet(
            names=names,
            timestamps=[100],
            rows=[empty_row()],
        )
        assert list(iter_row_proto(pkt, names)) == []

    def test_empty_rows_yields_nothing(self):
        names = ["vCar"]
        pkt = row_data_packet(names=names, timestamps=[], rows=[])
        assert list(iter_row_proto(pkt, names)) == []
        assert list(iter_row_proto(pkt, names, signals=frozenset({"vCar"}))) == []
