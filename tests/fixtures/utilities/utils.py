from concurrent.futures import ThreadPoolExecutor

import pytest


@pytest.fixture()
def executor() -> ThreadPoolExecutor:
    executor = ThreadPoolExecutor(1)
    yield executor
    # Kill all the threads after leaving the test
    executor.shutdown(wait=False)
