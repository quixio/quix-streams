import datetime
import unittest

from src.quixstreams.helpers.timeconverter import TimeConverter
from src.quixstreams import KafkaTimestamp, KafkaTimestampType


class KafkaTimestampTests(unittest.TestCase):

    @staticmethod
    def get_test_time():
        gmt_plus_2_offset = datetime.timedelta(hours=2)
        custom_timezone = datetime.timezone(gmt_plus_2_offset)
        dt = datetime.datetime(2016, 1, 5, 12, 23, 43, 578000, tzinfo=custom_timezone)
        dt_utc = datetime.datetime(2016, 1, 5, 10, 23, 43, 578000, tzinfo=datetime.timezone.utc)
        ns = TimeConverter.to_unix_nanoseconds(dt_utc)
        ms = round(ns / 1000000)
        return (dt, dt_utc, ms, ns)


    def test_constructor_value_with_datetime(self):
        # Arrange
        dt, dt_utc, ms, ns = KafkaTimestampTests.get_test_time()

        # Act
        timestamp = KafkaTimestamp(datetime=dt)
        # Assert
        self.assertEqual(dt_utc, timestamp.utc_datetime)
        self.assertEqual(ns, timestamp.unix_timestamp_ns)
        self.assertEqual(ms, timestamp.unix_timestamp_ms)

    def test_constructor_value_with_unis_ms(self):
        # Arrange
        dt, dt_utc, ms, ns = KafkaTimestampTests.get_test_time()

        # Act
        timestamp = KafkaTimestamp(unix_timestamp_ms=ms)
        timestamp2 = KafkaTimestamp(net_pointer=timestamp.get_net_pointer())

        # Assert
        self.assertEqual(dt_utc, timestamp.utc_datetime)
        self.assertAlmostEqual(ns, timestamp.unix_timestamp_ns, delta=1000000)  # precision loss of 1 ms
        self.assertEqual(ms, timestamp.unix_timestamp_ms)

    def test_constructor_value_with_unis_ns(self):
        # Arrange
        dt, dt_utc, ms, ns = KafkaTimestampTests.get_test_time()

        # Act
        timestamp = KafkaTimestamp(unix_timestamp_ns=ns)

        # Assert
        self.assertEqual(dt_utc, timestamp.utc_datetime)
        self.assertAlmostEqual(ns, timestamp.unix_timestamp_ns)
        self.assertEqual(ms, timestamp.unix_timestamp_ms)

    def test_constructor_type_properly_set(self):
        # Arrange
        dt, dt_utc, ms, ns = KafkaTimestampTests.get_test_time()

        # Act
        timestamp_ct = KafkaTimestamp(unix_timestamp_ns=ns, timestamp_type=KafkaTimestampType.CreateTime)
        timestamp_lat = KafkaTimestamp(unix_timestamp_ns=ns, timestamp_type=KafkaTimestampType.LogAppendTime)
        timestamp_nat = KafkaTimestamp(unix_timestamp_ns=ns, timestamp_type=KafkaTimestampType.NotAvailable)

        # Assert
        self.assertEqual(KafkaTimestampType.CreateTime, timestamp_ct.type)
        self.assertEqual(KafkaTimestampType.LogAppendTime, timestamp_lat.type)
        self.assertEqual(KafkaTimestampType.NotAvailable, timestamp_nat.type)


    def test_constructor_net_object_properly_set(self):
        # Arrange
        dt, dt_utc, ms, ns = KafkaTimestampTests.get_test_time()
        ts_orig_ct = KafkaTimestamp(datetime=dt, timestamp_type=KafkaTimestampType.CreateTime)
        ts_orig_lat = KafkaTimestamp(unix_timestamp_ms=ms, timestamp_type=KafkaTimestampType.LogAppendTime)
        ts_orig_nat = KafkaTimestamp(unix_timestamp_ns=ns, timestamp_type=KafkaTimestampType.NotAvailable)

        # Act
        timestamp_ct = KafkaTimestamp(net_pointer=ts_orig_ct.get_net_pointer())
        timestamp_lat = KafkaTimestamp(net_pointer=ts_orig_lat.get_net_pointer())
        timestamp_nat = KafkaTimestamp(net_pointer=ts_orig_nat.get_net_pointer())

        # Assert
        self.assertEqual(dt_utc, timestamp_ct.utc_datetime)
        self.assertEqual(ms, timestamp_lat.unix_timestamp_ms)
        self.assertAlmostEqual(ns, timestamp_nat.unix_timestamp_ns, delta=1000000)  # precision loss of 1 ms
        self.assertEqual(KafkaTimestampType.CreateTime, timestamp_ct.type)
        self.assertEqual(KafkaTimestampType.LogAppendTime, timestamp_lat.type)
        self.assertEqual(KafkaTimestampType.NotAvailable, timestamp_nat.type)




