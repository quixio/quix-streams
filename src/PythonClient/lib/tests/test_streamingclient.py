import unittest

from quixstreaming import StreamingClient, SecurityOptions, SaslMechanism
from tests.testbase import TestBase


class StreamingClientTests(unittest.TestCase):

    def test_constructor_with_topic(self):
        # Act
        sc = StreamingClient("my-broker");
        # Assert by no exception

    def test_constructor_with_securityoptions(self):

        mechanisms = [None, SaslMechanism.Plain, SaslMechanism.ScramSha256, SaslMechanism.ScramSha512] # , SaslMechanism.Gssapi, SaslMechanism.OAuthBearer not supported
        for mechanism in mechanisms:
            # Arrange
            so = SecurityOptions(TestBase.get_certpath(), "username", "password", mechanism)
            # Act
            sc = StreamingClient("my-broker", security_options=so)
            # Assert by no exception

    def test_constructor_with_properties(self):
        # Act
        sc = StreamingClient("my-broker", properties={"acks": "0"})
        # Assert by no exception

    def test_constructor_with_debug(self):
        # Act
        sc = StreamingClient("my-broker", debug=True)
        # Assert by no exception