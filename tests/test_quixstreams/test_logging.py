import logging
from unittest.mock import patch

import pytest

import quixstreams.logging as logging_module
from quixstreams.logging import configure_logging


@pytest.fixture()
def reset_state_override():
    """Snapshot + restore the module-global ``quixstreams.state`` override state so
    the #15 tests don't leak the dedicated handler / level into other tests."""
    state_logger = logging.getLogger(logging_module._STATE_LOGGER_NAME)
    saved = (
        state_logger.handlers[:],
        state_logger.level,
        state_logger.propagate,
        logging_module._STATE_HANDLER,
    )
    if logging_module._STATE_HANDLER is not None:
        state_logger.removeHandler(logging_module._STATE_HANDLER)
    logging_module._STATE_HANDLER = None
    state_logger.handlers = []
    state_logger.setLevel(logging.NOTSET)
    state_logger.propagate = True
    yield
    state_logger.handlers, level, state_logger.propagate, handler = (
        saved[0],
        saved[1],
        saved[2],
        saved[3],
    )
    state_logger.setLevel(level)
    logging_module._STATE_HANDLER = handler


class TestConfigureLogging:
    def test_configure_logging_no_handlers_defined(self, monkeypatch):
        with patch(
            "quixstreams.logging.logger",
        ) as mock:
            mock.handlers = []
            updated = configure_logging("INFO")
            assert updated

    def test_configure_logging_handlers_already_defined(self):
        with patch(
            "quixstreams.logging.logger",
        ) as mock:
            mock.handlers = [123]
            updated = configure_logging("INFO")
            assert not updated

    def test_configure_logging_loglevel_none(self):
        with patch(
            "quixstreams.logging.logger",
        ) as mock:
            mock.handlers = []
            updated = configure_logging(None)
            assert not updated


class TestStateLogLevelOverride:
    """#15 (review batch 3): ``QUIXSTREAMS_STATE_LOG_LEVEL`` must be honored on
    EVERY return path of ``configure_logging`` — including when the app owns the
    ``quixstreams`` handlers (the early ``return False``) and when ``loglevel is
    None`` — not only when we (re)configure the default handler."""

    def test_override_applies_with_custom_app_handler(
        self, monkeypatch, reset_state_override
    ):
        monkeypatch.setenv("QUIXSTREAMS_STATE_LOG_LEVEL", "DEBUG")
        with patch("quixstreams.logging.logger") as mock:
            mock.handlers = [logging.NullHandler()]  # app owns a non-default handler
            updated = configure_logging("INFO")
        assert updated is False  # app-owned handler path returns False
        state_logger = logging.getLogger(logging_module._STATE_LOGGER_NAME)
        assert state_logger.level == logging.DEBUG
        assert logging_module._STATE_HANDLER is not None
        assert logging_module._STATE_HANDLER in state_logger.handlers

    def test_override_applies_when_loglevel_none(
        self, monkeypatch, reset_state_override
    ):
        monkeypatch.setenv("QUIXSTREAMS_STATE_LOG_LEVEL", "DEBUG")
        updated = configure_logging(None)
        assert updated is False
        state_logger = logging.getLogger(logging_module._STATE_LOGGER_NAME)
        assert state_logger.level == logging.DEBUG
        assert logging_module._STATE_HANDLER is not None

    def test_override_unset_is_noop_on_none_path(
        self, monkeypatch, reset_state_override
    ):
        monkeypatch.delenv("QUIXSTREAMS_STATE_LOG_LEVEL", raising=False)
        configure_logging(None)
        state_logger = logging.getLogger(logging_module._STATE_LOGGER_NAME)
        assert state_logger.level == logging.NOTSET
        assert logging_module._STATE_HANDLER is None

    def test_custom_handler_still_receives_state_records_when_override_set(
        self, monkeypatch, reset_state_override
    ):
        """Finding 3: with ``QUIXSTREAMS_STATE_LOG_LEVEL`` set, a custom handler
        on the ``quixstreams`` logger must still receive ``quixstreams.state.*``
        records -- i.e. the state-log override must be additive, NOT steal
        propagation from custom handlers on the parent.

        Validates spec: state-log override preserves propagation to parent
        logger's handlers.
        """
        monkeypatch.setenv("QUIXSTREAMS_STATE_LOG_LEVEL", "DEBUG")

        # Attach a capturing handler to the parent ``quixstreams`` logger.
        parent_logger = logging.getLogger("quixstreams")
        captured: list[logging.LogRecord] = []

        class _Capture(logging.Handler):
            def emit(self, record):
                captured.append(record)

        handler = _Capture()
        handler.setLevel(logging.DEBUG)
        parent_logger.addHandler(handler)
        try:
            # Apply the state-log override (same path as configure_logging's
            # ``finally`` block).
            logging_module._apply_state_log_level_override()

            # Log an ERROR on a quixstreams.state child logger.
            state_child = logging.getLogger("quixstreams.state.test_propagation")
            state_child.error("test-error-finding-3")

            assert any(r.getMessage() == "test-error-finding-3" for r in captured), (
                "custom handler on 'quixstreams' must receive "
                "'quixstreams.state.*' ERROR records; propagation was stolen "
                "by the state-log override (propagate=False)"
            )
        finally:
            parent_logger.removeHandler(handler)
