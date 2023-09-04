import threading
import logging
from typing import Dict, Callable, Any

from .dictstreamstate import DictStreamState
from .scalarstreamstate import ScalarStreamState
from ..statestorages import IStateStorage
from ..statestorages import StreamStateType


class StreamStateManager:
    def __init__(self, stream_id: str, state_storage: IStateStorage, log_prefix: str, topic_consumer=None):
        self._topic_consumer = topic_consumer
        self._log_prefix = f"{log_prefix}{stream_id}"
        self._stream_id = stream_id
        self._state_storage = state_storage
        self._state_lock = threading.Lock()
        self._states = {}  # type: Dict[str, IStreamState]

    def get_states(self):
        return self._state_storage.get_sub_storages()

    def delete_states(self):
        # self._logger.log(logging.DEBUG, f"Deleting Stream states for {self._stream_id}")
        count = self._state_storage.delete_sub_storages()
        self._states.clear()
        return count

    def delete_state(self, state_name):
        # self._logger.log(logging.DEBUG, f"Deleting Stream state {state_name} for {self._stream_id}")
        if not self._state_storage.delete_sub_storage(state_name):
            return False
        del self._states[state_name]
        return True

    def _get_stream_state(self, state_name, create_state):
        if state_name in self._states:
            existing_state = self._states[state_name]
            if not isinstance(existing_state, create_state(state_name).__class__):
                raise Exception(
                    f"{self._log_prefix}, State '{state_name}' already exists as different stream state type.")
            return existing_state
        with self._state_lock:
            if state_name in self._states:
                existing_state = self._states[state_name]
                if not isinstance(existing_state, create_state(state_name).__class__):
                    raise Exception(
                        f"{self._log_prefix}, State '{state_name}' already exists as different stream state type.")
                return existing_state
            state = create_state(state_name)
            self._states[state_name] = state

            if self._topic_consumer is None:
                return state

            prefix = f"{self._log_prefix} - {state_name} | "

            def committed_handler(sender, args):
                try:
                    # self._logger.log(logging.DEBUG, f"{prefix} | Topic committing, flushing state.")
                    state.flush()
                    # self._logger.log(logging.DEBUG, f"{prefix} | Topic committing, flushed state.")
                except Exception as ex:
                    # self._logger.log(logging.ERROR, f"{prefix} | Failed to flush state.", ex)
                    pass

            self._topic_consumer.on_committed = committed_handler

            def on_streams_revoked(sender, consumers):
                if all(y.stream_id != self._stream_id for y in consumers):
                    return
                # self._logger.log(logging.DEBUG, f"{prefix} | Stream revoked, discarding state.")
                self._topic_consumer.on_committed = None
                state.reset()

            self._topic_consumer.on_streams_revoked = on_streams_revoked

            return state

    def get_dict_state(self, state_name: str, default_value_factory: Callable[[str], StreamStateType] = None, state_type: StreamStateType = None) -> DictStreamState[StreamStateType]:
        """
        Creates a new application state of dictionary type with automatically managed lifecycle for the stream

        Args:
            state_name: The name of the state
            state_type: The type of the state
            default_value_factory: The default value factory to create value when the key is not yet present in the state

        Example:
            >>> state_manager.get_dict_state('some_state')
            This will return a state where type is 'Any'

            >>> state_manager.get_dict_state('some_state', lambda missing_key: return {})
            this will return a state where type is a generic dictionary, with an empty dictionary as default value when
            key is not available. The lambda function will be invoked with 'get_state_type_check' key to determine type

            >>> state_manager.get_dict_state('some_state', lambda missing_key: return {}, Dict[str, float])
            this will return a state where type is a specific dictionary type, with default value

            >>> state_manager.get_dict_state('some_state', state_type=float)
            this will return a state where type is a float without default value, resulting in KeyError when not found
        """

        if state_type is None:
            state_type = Any
            # Try to figure out the type based on python typehints
            if default_value_factory is not None:
                try:
                    state_type = type(default_value_factory('get_state_type_check'))
                    if state_type is None:
                        state_type = Any
                except:
                    pass

        instance: DictStreamState = self._states.get(state_name)
        if instance is not None:
            if instance.type is not state_type:
                logging.log(logging.WARNING, f'State {state_name} already exists with a different type ({instance.type.__name__}), new type is {state_type.__name__}. Returning original state instance.')
            return instance

        instance = self._get_stream_state(state_name, lambda name: DictStreamState[StreamStateType](
            storage=self._state_storage.get_or_create_sub_storage(name), state_type=state_type, default_value_factory=default_value_factory))
        self._states[state_name] = instance
        return instance

    def get_scalar_state(self, state_name: str, default_value_factory: Callable[[], StreamStateType] = None, state_type: StreamStateType = None) -> ScalarStreamState[StreamStateType]:
        """
        Creates a new application state of scalar type with automatically managed lifecycle for the stream

        Args:
            state_name: The name of the state
            default_value_factory: The default value factory to create value when it has not been set yet
            state_type: The type of the state

        Example:
            >>> stream_consumer.get_scalar_state('some_state')
            This will return a state where type is 'Any'

            >>> stream_consumer.get_scalar_state('some_state', lambda: return 1)
            this will return a state where type is 'Any', with an integer 1 (zero) as default when
            value has not been set yet. The lambda function will be invoked with 'get_state_type_check' key to determine type

            >>> stream_consumer.get_scalar_state('some_state', lambda: return 0, float)
            this will return a state where type is a specific type, with default value

            >>> stream_consumer.get_scalar_state('some_state', state_type=float)
            this will return a state where type is a float with a default value of that type
        """

        if state_type is None:
            state_type = Any
            # Try to figure out the type based on python typehints
            if default_value_factory is not None:
                try:
                    state_type = type(default_value_factory('get_state_type_check'))
                    if state_type is None:
                        state_type = Any
                except:
                    pass

        instance: ScalarStreamState = self._states.get(state_name)
        if instance is not None:
            if instance.type is not state_type:
                logging.log(logging.WARNING, f'State {state_name} already exists with a different type ({instance.type.__name__}), new type is {state_type.__name__}. Returning original state instance.')
            return instance

        instance = self._get_stream_state(state_name, lambda name: ScalarStreamState[StreamStateType](
            storage=self._state_storage.get_or_create_sub_storage(name), state_type=state_type, default_value_factory=default_value_factory))
        self._states[state_name] = instance
        return instance

