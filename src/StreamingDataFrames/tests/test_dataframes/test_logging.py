from unittest.mock import patch

from streamingdataframes.logging import configure_logging


class TestConfigureLogging:
    def test_configure_logging_no_handlers_defined(self, monkeypatch):
        with patch(
            "streamingdataframes.logging.logger",
        ) as mock:
            mock.handlers = []
            updated = configure_logging("INFO")
            assert updated

    def test_configure_logging_handlers_already_defined(self):
        with patch(
            "streamingdataframes.logging.logger",
        ) as mock:
            mock.handlers = [123]
            updated = configure_logging("INFO")
            assert not updated

    def test_configure_logging_loglevel_none(self):
        with patch(
            "streamingdataframes.logging.logger",
        ) as mock:
            mock.handlers = []
            updated = configure_logging(None)
            assert not updated
