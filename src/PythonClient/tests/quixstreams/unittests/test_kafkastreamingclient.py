import unittest

from testhelper import TestHelper
from src.quixstreams import KafkaStreamingClient, SecurityOptions, SaslMechanism

class KafkaStreamingClientTests(unittest.TestCase):

    def test_constructor_with_topic(self):
        # Act
        sc = KafkaStreamingClient("my-broker");
        # Assert by no exception

    def test_constructor_with_securityoptions(self):

        mechanisms = [None, SaslMechanism.Plain, SaslMechanism.ScramSha256, SaslMechanism.ScramSha512] # , SaslMechanism.Gssapi, SaslMechanism.OAuthBearer not supported
        for mechanism in mechanisms:
            # Arrange
            so = SecurityOptions(TestHelper.get_certpath(), "username", "password", mechanism)
            # Act
            sc = KafkaStreamingClient("my-broker", security_options=so)
            # Assert by no exception

    def test_constructor_with_properties(self):
        # Act
        sc = KafkaStreamingClient("my-broker", properties={"acks": "0"})
        # Assert by no exception

    def test_constructor_with_debug(self):
        # Act
        sc = KafkaStreamingClient("my-broker", debug=True)
        # Assert by no exception