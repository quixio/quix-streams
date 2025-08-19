import enum
from abc import ABC, abstractmethod
from typing import Any

from quixstreams.state.base.state import State

__all__ = ("TriggerAction", "TimeWindowTrigger")


class TriggerAction(enum.Enum):
    EMIT = 1
    CONTINUE = 2
    DELETE = 3
    EMIT_AND_DELETE = 4


# TODO: Should we support multiple triggers? Or rather a single trigger that can react to many events?
# TODO: If we skip a window update, we still must bump the event time (!)
#       It means that we must detach time bump from the window update
# TODO: The trigger state can live in a pre-defined column family "__trigger__"
class TimeWindowTrigger(ABC):
    @abstractmethod
    def on_window_closed(self) -> TriggerAction:
        """
        Called after the window is already closed and removed from the state.

        TODO: "delete" and "emit-and-delete" actions don't make sense here
        """

    @abstractmethod
    def on_window_updated(
        self,
        value: Any,
        start: int,
        end: int,
        state: State,
    ) -> TriggerAction:
        # TODO: This looks like "on_window_update"
        # TODO: Is it possible to support "collect()" for this one?
        """
        Called after the window is updated.
        """
        ...

    @abstractmethod
    def clear(self, state: State):
        # TODO: Clear the trigger state when the window is closed
        #  It's probably better to make the trigger to define it to avoid querying the whole CF for keys to delete.
        ...

    # @abstractmethod
    # def on_wallclock_time_advanced(
    #     self, timestamp_ms: int, state: State
    # ) -> TriggerAction:
    #     """
    #     Called when the heartbeat is received.
    #     Can be used to trigger the processing-time based behavior
    #     when there're no new messages for some time.
    #
    #     # TODO: The granularity of the heartbeats is regulated elsewhere
    #     # TODO: If we want to expire windows based on processing time,
    #         how do we avoid re-scanning the state on every heartbeat?
    #         1. The default implementation is void
    #         2. In "on_window_updated", we need to add a window key to a separate state to know when it's added
    #         3. The state should have a cache on top of it, which is some heapq initialized with a limited number of keys (e.g. 10000)
    #            The queue is needed to limit the additional memory use.
    #            When the queue gets empty (e.g. the first 10000 windows are expired), we refill it with another 10000.
    #         4. How do we even emit or expire windows from this trigger since we don't have them? Should this method have a different API?
    #            If we store the keys to be expired, could we also return them from this method?
    #
    #     """
