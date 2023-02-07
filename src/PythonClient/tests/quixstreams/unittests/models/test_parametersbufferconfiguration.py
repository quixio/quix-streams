import unittest

from src.quixstreams import ParameterData

from src.quixstreams.models.parameterdatatimestamp import ParameterDataTimestamp
from src.quixstreams.models.parametersbufferconfiguration import ParametersBufferConfiguration


class ParametersBufferConfigurationTests(unittest.TestCase):

    def test_packetsize_gets_sets(self):
        # Arrange
        buffer_config = ParametersBufferConfiguration()

        # Act
        buffer_config.packet_size = 2

        # Assert
        self.assertEqual(2, buffer_config.packet_size)

    def test_timespan_in_milliseconds_gets_sets(self):
        # Arrange
        buffer_config = ParametersBufferConfiguration()

        # Act
        buffer_config.time_span_in_milliseconds = 3

        # Assert
        self.assertEqual(3, buffer_config.time_span_in_milliseconds)
        self.assertEqual(3000000, buffer_config.time_span_in_nanoseconds)

    def test_timespan_in_nanoseconds_gets_sets(self):
        # Arrange
        buffer_config = ParametersBufferConfiguration()

        # Act
        buffer_config.time_span_in_nanoseconds = 3000000

        # Assert
        self.assertEqual(3, buffer_config.time_span_in_milliseconds)
        self.assertEqual(3000000, buffer_config.time_span_in_nanoseconds)

    def test_timeout_gets_sets(self):
        # Arrange
        buffer_config = ParametersBufferConfiguration()

        # Act
        buffer_config.buffer_timeout = 300

        # Assert
        self.assertEqual(300, buffer_config.buffer_timeout)

    def test_custom_trigger_before_enqueue_gets_sets(self):
        # Arrange
        buffer_config = ParametersBufferConfiguration()

        # Act
        self.assertIsNone(buffer_config.custom_trigger_before_enqueue)

        def custom_trigger(ts: ParameterDataTimestamp) -> bool:
            pass

        buffer_config.custom_trigger_before_enqueue = custom_trigger

        # Assert
        self.assertEqual(custom_trigger, buffer_config.custom_trigger_before_enqueue)

    def test_custom_trigger_before_enqueue_from_netobject(self):
        # Arrange
        # what we are doing here is setting up a .net underlying object with an existing custom_trigger_before_enqueue func
        buffer_config = ParametersBufferConfiguration()

        def custom_trigger(ts: ParameterDataTimestamp) -> bool:
            pass

        buffer_config.custom_trigger_before_enqueue = custom_trigger

        # Act
        buffer_config = ParametersBufferConfiguration(buffer_config.get_net_pointer())

        # Assert
        self.assertIsNotNone(buffer_config.custom_trigger_before_enqueue)

    def test_filter_gets_sets(self):
        # Arrange
        buffer_config = ParametersBufferConfiguration()

        # Act
        self.assertIsNone(buffer_config.filter)

        def custom_trigger(ts: ParameterDataTimestamp) -> bool:
            pass

        buffer_config.filter = custom_trigger

        # Assert
        self.assertEqual(custom_trigger, buffer_config.filter)

    def test_filter_from_netobject(self):
        # Arrange
        # what we are doing here is setting up a .net underlying object with an existing filter func
        buffer_config = ParametersBufferConfiguration()

        def custom_trigger(ts: ParameterDataTimestamp) -> bool:
            pass

        buffer_config.filter = custom_trigger

        # Act
        buffer_config = ParametersBufferConfiguration(buffer_config.get_net_pointer())

        # Assert
        self.assertIsNotNone(buffer_config.filter)
        
    def test_custom_trigger_gets_sets(self):
        # Arrange
        buffer_config = ParametersBufferConfiguration()

        # Act
        self.assertIsNone(buffer_config.custom_trigger)

        def custom_trigger(ts: ParameterDataTimestamp) -> bool:
            pass

        buffer_config.custom_trigger = custom_trigger

        # Assert
        self.assertEqual(custom_trigger, buffer_config.custom_trigger)

    def test_custom_trigger_from_netobject(self):
        # Arrange
        # what we are doing here is setting up a .net underlying object with an existing custom_trigger func
        buffer_config = ParametersBufferConfiguration()

        def custom_trigger(pd: ParameterData) -> bool:
            pass

        buffer_config.custom_trigger = custom_trigger

        # Act
        buffer_config = ParametersBufferConfiguration(buffer_config.get_net_pointer())

        # Assert
        self.assertIsNotNone(buffer_config.custom_trigger)
