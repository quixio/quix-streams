import pytest
import time
import signal

from quixstreams.models.topics import Topic
from quixstreams.sources.manager import SourceManager, SourceException, multiprocessing

from tests.utils import DummySource


class TestSourceManager:
    def test_register(self):
        manager = SourceManager()

        source1 = DummySource(name="source1")
        source2 = DummySource(name="source2")

        topic1 = Topic("topic1", None)
        topic2 = Topic("topic2", None)

        with pytest.raises(ValueError):
            manager.register(source1)

        source1.configure(topic1, None)
        manager.register(source1)

        # registering the same source twice fails
        with pytest.raises(ValueError):
            manager.register(source1)

        source2.configure(topic1, None)
        # registering a source with the same topic fails
        with pytest.raises(ValueError):
            manager.register(source2)

        source2.configure(topic2, None)
        manager.register(source2)

    def test_is_alives(self):
        manager = SourceManager()
        source = DummySource()

        source.configure(Topic("topic", None), None)
        manager.register(source)

        assert not manager.is_alive()

        manager.start_sources()
        assert manager.is_alive()

        manager.stop_sources()
        assert not manager.is_alive()

    def test_is_alives_kill_source(self):
        manager = SourceManager()
        source = DummySource()

        source.configure(Topic("topic", None), None)
        process = manager.register(source)

        assert not manager.is_alive()

        manager.start_sources()
        time.sleep(0.01)

        pid = process.pid

        process.kill()
        time.sleep(0.01)
        assert not manager.is_alive()

        with pytest.raises(SourceException) as exc:
            manager.raise_for_error()

        assert exc.value.exitcode == -9
        assert exc.value.pid == pid

    def test_terminate_source(self):
        manager = SourceManager()
        source = DummySource()

        source.configure(Topic("topic", None), None)
        process = manager.register(source)

        assert not manager.is_alive()

        manager.start_sources()
        time.sleep(0.01)

        process.terminate()
        time.sleep(0.2)
        assert not manager.is_alive()

        manager.raise_for_error()
        assert process.exitcode == -signal.SIGTERM

    @pytest.mark.parametrize(
        "when,exitcode", [("run", 0), ("cleanup", 0), ("stop", -9)]
    )
    @pytest.mark.parametrize("pickleable", [True, False])
    def test_raise_for_error_run(self, when, exitcode, pickleable):
        manager = SourceManager()

        finished = multiprocessing.Event()
        source = DummySource(
            error_in=when, pickeable_error=pickleable, finished=finished
        )

        source.configure(Topic("topic", None), None)
        process = manager.register(source)

        manager.start_sources()

        finished.wait(1)
        pid = process.pid

        with pytest.raises(SourceException) as exc:
            manager.stop_sources()

        assert exc.value.exitcode == exitcode
        assert exc.value.pid == pid
        assert exc.value.__cause__
        if pickleable:
            assert isinstance(exc.value.__cause__, ValueError)
        else:
            assert isinstance(exc.value.__cause__, RuntimeError)
        assert str(exc.value.__cause__) == f"test {when} error"
