from quixstreaming import StreamingClient, SecurityOptions, Logging, LogLevel
import os


class TestBase(object):

    local = True

    @staticmethod
    def get_certpath() -> str:
        return os.path.abspath(os.path.join(__file__, "../../../../TestCertificates/ca.cert"))

    @staticmethod
    def create_streaming_client() -> StreamingClient:
        Logging.update_factory(LogLevel.Trace)
        if TestBase.local:
            client = StreamingClient('127.0.0.1:9092', None, debug=False)
        else:
            certPath = TestBase.get_certpath()
            security = SecurityOptions(certPath, "sasl-username", "sasl-password")
            client = StreamingClient("whateverbroker", security)
        return client

    @staticmethod
    def get_test_topic() -> str:
        if TestBase.local:
            return "integration-tests-python"
        else:
            return "integration-tests"

    @staticmethod
    def get_consumer_group() -> str:
        return "quix.tests-python-sdk-tests"