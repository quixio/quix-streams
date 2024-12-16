import logging
import logging.config

import pytest

from tests.utilities.logging import LOGGING_CONFIG, patch_logger_class

test_logger = logging.getLogger("quixstreams.tests")


@pytest.fixture(autouse=True, scope="session")
def configure_logging():
    logging.config.dictConfig(LOGGING_CONFIG)
    patch_logger_class()


@pytest.fixture(autouse=True)
def log_test_progress(request: pytest.FixtureRequest):
    test_logger.debug("Starting test %s", request.node.nodeid)
