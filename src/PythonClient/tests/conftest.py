# Define list of files with fixtures for pytest autodiscovery
pytest_plugins = [
    "tests.quixstreams.test_dataframes.test_kafka.fixtures",
    "tests.quixstreams.test_dataframes.test_dataframe.fixtures",
]
