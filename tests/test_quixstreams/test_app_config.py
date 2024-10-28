import pydantic
import pytest


class TestApplicationConfig:
    def test_frozen(self, app_factory):
        app = app_factory()

        with pytest.raises(pydantic.ValidationError):
            app.config.consumer_group = "foo"

    def test_notequal(self, app_factory):
        app1 = app_factory()
        app2 = app_factory(consumer_group="foo")

        assert app1.config != app2.config

    def test_copy(self, app_factory):
        app = app_factory()
        assert app.config == app.config.copy()

        with pytest.raises(pydantic.ValidationError):
            app.config.copy(foo=1)

        with pytest.raises(pydantic.ValidationError):
            app.config.copy(processing_guarantee="foo")

    def test_update(self, app_factory):
        app = app_factory()

        copy = app.config.copy(consumer_group="foo")
        assert app.config != copy
        assert app.config.consumer_group != "foo"
        assert copy.consumer_group == "foo"

    def test_exactly_once(self, app_factory):
        app = app_factory()

        assert not app.config.exactly_once
        assert app.config.copy(processing_guarantee="exactly-once").exactly_once

    def test_flush_timeout(self, app_factory):
        app = app_factory()

        assert app.config.flush_timeout == 300.0
        assert (
            app.config.copy(
                consumer_extra_config={"max.poll.interval.ms": 1000}
            ).flush_timeout
            == 1
        )
