from quixstreams.app import Application


def test_application_accepts_slatedb_backend():
    # Do not run the app; just construct to ensure wiring does not crash
    app = Application(
        broker_address="localhost:9092",
        consumer_group="smoke",
        state_dir="/tmp/quix-state",
        state_backend="slatedb",
    )
    assert app.config.state_backend == "slatedb"
