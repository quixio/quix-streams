"""Unit tests for the fast-path sample iterators (sample_iter.py).

Validates sample_iter.py's own docstring contract (signal filtering
skips whole columns/rows before the per-sample loop) and
docs/advanced/serialization.md:150 ("Per-sample timestamps are
reconstructed from the wire format: start_time + i * interval for
PeriodicData, cumulative intervals for SynchroData, per-row
timestamps[] for RowData").
"""

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
        """Validates serialization.md:150 timestamp formula for PeriodicData."""
        pkt = periodic_data_packet(
            names=["vCar"],
            start_time=1000,
            interval=10,
            columns=[double_column([(1.0, VALID), (2.0, VALID), (3.0, VALID)])],
        )
        rows = list(iter_periodic_proto(pkt))
        assert rows == [
            ("vCar", 1000, 1.0, "DATA_STATUS_VALID"),
            ("vCar", 1010, 2.0, "DATA_STATUS_VALID"),
            ("vCar", 1020, 3.0, "DATA_STATUS_VALID"),
        ]

    def test_multiple_columns_no_signal_filter(self):
        pkt = periodic_data_packet(
            names=["vCar", "nEngine"],
            start_time=0,
            interval=1,
            columns=[
                double_column([(1.0, VALID)]),
                double_column([(2.0, MISSING)]),
            ],
        )
        rows = list(iter_periodic_proto(pkt))
        assert rows == [
            ("vCar", 0, 1.0, "DATA_STATUS_VALID"),
            ("nEngine", 0, 2.0, "DATA_STATUS_MISSING"),
        ]

    def test_signal_filter_keeps_only_allowlisted_columns(self):
        pkt = periodic_data_packet(
            names=["vCar", "nEngine"],
            start_time=0,
            interval=1,
            columns=[
                double_column([(1.0, VALID)]),
                double_column([(2.0, VALID)]),
            ],
        )
        rows = list(iter_periodic_proto(pkt, signals=frozenset({"nEngine"})))
        assert rows == [("nEngine", 0, 2.0, "DATA_STATUS_VALID")]

    def test_signal_filter_matching_nothing_yields_no_rows(self):
        pkt = periodic_data_packet(
            names=["vCar"],
            start_time=0,
            interval=1,
            columns=[double_column([(1.0, VALID)])],
        )
        assert list(iter_periodic_proto(pkt, signals=frozenset({"missing"}))) == []

    def test_column_with_unset_oneof_yields_no_samples(self):
        pkt = periodic_data_packet(
            names=["vCar"],
            start_time=0,
            interval=1,
            columns=[empty_column()],
        )
        assert list(iter_periodic_proto(pkt)) == []


class TestIterSynchroProto:
    def test_reconstructs_cumulative_timestamps(self):
        """Validates serialization.md:150: cumulative intervals for SynchroData."""
        pkt = synchro_data_packet(
            names=["vCar"],
            start_time=1000,
            intervals=[5, 7],
            columns=[double_column([(1.0, VALID), (2.0, VALID), (3.0, VALID)])],
        )
        rows = list(iter_synchro_proto(pkt))
        assert rows == [
            ("vCar", 1000, 1.0, "DATA_STATUS_VALID"),
            ("vCar", 1005, 2.0, "DATA_STATUS_VALID"),
            ("vCar", 1012, 3.0, "DATA_STATUS_VALID"),
        ]

    def test_samples_beyond_intervals_fall_back_to_last_cumulative_value(self):
        """More samples than intervals: sample_iter.py:72 falls back to
        the last cumulative timestamp instead of raising/misindexing.
        """
        pkt = synchro_data_packet(
            names=["vCar"],
            start_time=0,
            intervals=[10],
            columns=[double_column([(1.0, VALID), (2.0, VALID), (3.0, VALID)])],
        )
        rows = list(iter_synchro_proto(pkt))
        assert [ts for _, ts, _, _ in rows] == [0, 10, 10]

    def test_signal_filter_keeps_only_allowlisted_columns(self):
        pkt = synchro_data_packet(
            names=["vCar", "nEngine"],
            start_time=0,
            intervals=[1],
            columns=[
                double_column([(1.0, VALID)]),
                double_column([(2.0, VALID)]),
            ],
        )
        rows = list(iter_synchro_proto(pkt, signals=frozenset({"vCar"})))
        assert rows == [("vCar", 0, 1.0, "DATA_STATUS_VALID")]


class TestIterRowProto:
    def test_reconstructs_per_row_timestamps(self):
        """Validates serialization.md:150: per-row timestamps[] for RowData."""
        pkt = row_data_packet(
            names=["vCar", "nEngine"],
            timestamps=[100, 200],
            rows=[
                double_row([(1.0, VALID), (10.0, VALID)]),
                double_row([(2.0, VALID), (20.0, VALID)]),
            ],
        )
        rows = list(iter_row_proto(pkt))
        assert rows == [
            ("vCar", 100, 1.0, "DATA_STATUS_VALID"),
            ("nEngine", 100, 10.0, "DATA_STATUS_VALID"),
            ("vCar", 200, 2.0, "DATA_STATUS_VALID"),
            ("nEngine", 200, 20.0, "DATA_STATUS_VALID"),
        ]

    def test_signal_filter_keeps_only_allowlisted_columns_per_row(self):
        pkt = row_data_packet(
            names=["vCar", "nEngine"],
            timestamps=[100],
            rows=[double_row([(1.0, VALID), (10.0, VALID)])],
        )
        rows = list(iter_row_proto(pkt, signals=frozenset({"nEngine"})))
        assert rows == [("nEngine", 100, 10.0, "DATA_STATUS_VALID")]

    def test_signal_filter_matching_nothing_yields_no_rows(self):
        pkt = row_data_packet(
            names=["vCar"],
            timestamps=[100],
            rows=[double_row([(1.0, VALID)])],
        )
        assert list(iter_row_proto(pkt, signals=frozenset({"missing"}))) == []

    def test_row_with_unset_oneof_is_skipped(self):
        pkt = row_data_packet(
            names=["vCar"],
            timestamps=[100],
            rows=[empty_row()],
        )
        assert list(iter_row_proto(pkt)) == []

    def test_empty_rows_yields_nothing(self):
        pkt = row_data_packet(names=["vCar"], timestamps=[], rows=[])
        assert list(iter_row_proto(pkt)) == []
        assert list(iter_row_proto(pkt, signals=frozenset({"vCar"}))) == []
