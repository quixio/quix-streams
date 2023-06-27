import unittest

import pytest

import src.quixstreams as qx


from src.quixstreams.native.Python.InteropHelpers.InteropUtils import InteropUtils
InteropUtils.enable_debug()
qx.Logging.update_factory(qx.LogLevel.Debug)


class AppTests(unittest.TestCase):

    @pytest.mark.skip('Test fails with "App state manager may only be set once"')
    def test_set_state_manager(self):
        # Arrange
        mem_storage = qx.InMemoryStorage()

        # Act
        qx.App.set_state_storage(mem_storage)

        # Assert
        app_state_manager = qx.App.get_state_manager()

        self.assertIsNotNone(app_state_manager)

        mem_storage.get_or_create_sub_storage("some_random_topic")  # create a fake topic inside state
        topic_states = app_state_manager.get_topic_states()
        self.assertIn("some_random_topic", topic_states)   # by doing this we assert the mem storage is actually used

    def test_get_state_manager(self):
        # Act
        app_state_manager = qx.App.get_state_manager()

        self.assertIsNotNone(app_state_manager)

        topic_manager = app_state_manager.get_topic_state_manager('test')
