from __future__ import annotations

import contextlib
import contextvars
import functools
import operator
from datetime import timedelta
from typing import (
    Optional,
    Callable,
    Union,
    List,
    TypeVar,
    Any,
    overload,
    Dict,
    TYPE_CHECKING,
)
from unittest.mock import patch

from typing_extensions import Self

from quixstreams.context import (
    message_context,
    set_message_context,
    message_key,
)
from quixstreams.core.stream import StreamCallable, Stream
from quixstreams.models import (
    Topic,
    TopicManager,
    Row,
    MessageContext,
    MessageTimestamp,
    TimestampType,
)
from quixstreams.models.serializers import SerializerType, DeserializerType
from quixstreams.rowproducer import RowProducerProto
from quixstreams.state import StateStoreManager, State
from .base import BaseStreaming
from .exceptions import InvalidOperation
from .series import StreamingSeries
from .utils import ensure_milliseconds
from .windows import TumblingWindowDefinition, HoppingWindowDefinition

if TYPE_CHECKING:
    from quixstreams import Application

T = TypeVar("T")
R = TypeVar("R")
DataFrameFunc = Callable[[T], R]
DataFrameStatefulFunc = Callable[[T, State], R]


class StreamingDataFrame(BaseStreaming):
    """
    `StreamingDataFrame` is the main object you will use for ETL work.

    Typically created with an `app = quixstreams.app.Application()` instance,
    via `sdf = app.dataframe()`.


    What it Does:

    - Builds a data processing pipeline, declaratively (not executed immediately)
        - Executes this pipeline on inputs at runtime (Kafka message values)
    - Provides functions/interface similar to Pandas Dataframes/Series
    - Enables stateful processing (and manages everything related to it)


    How to Use:

    Define various operations while continuously reassigning to itself (or new fields).

    These operations will generally transform your data, access/update state, or produce
    to kafka topics.

    We recommend your data structure to be "columnar" (aka a dict/JSON) in nature so
    that it works with the entire interface, but simple types like `ints`, `str`, etc.
    are also supported.

    See the various methods and classes for more specifics, or for a deep dive into
    usage, see `streamingdataframe.md` under the `docs/` folder.

    >***NOTE:*** column referencing like `sdf["a_column"]` and various methods often
        create other object types (typically `quixstreams.dataframe.StreamingSeries`),
        which is expected; type hinting should alert you to any issues should you
        attempt invalid operations with said objects (however, we cannot infer whether
        an operation is valid with respect to your data!).


    Example Snippet:

    ```python
    sdf = StreamingDataframe()
    sdf = sdf.apply(a_func)
    sdf = sdf.filter(another_func)
    sdf = sdf.to_topic(topic_obj)
    ```
    """

    def __init__(
        self,
        topic: Topic,
        application: Application,
        stream: Optional[Stream] = None,
        branches: Optional[Dict[str, Self]] = None,
        stateful: bool = False,
    ):
        self._stream: Stream = stream or Stream()
        self._topic = topic
        self._application = application
        self._branches = branches or {}
        self._stateful = stateful
        if self._topic.name not in self._branches:
            self._branches[self._topic.name] = self
        self._real_producer: Optional[RowProducerProto] = None

    @property
    def stream(self) -> Stream:
        return self._stream

    @property
    def topic(self) -> Topic:
        return self._topic

    @property
    def is_stateful(self) -> bool:
        return self._stateful

    @property
    def _topic_manager(self) -> TopicManager:
        return self._application._topic_manager

    @property
    def state_manager(self) -> StateStoreManager:
        return self._application._state_manager

    @property
    def consumer_topics(self) -> List[Topic]:
        return [sdf.topic for sdf in self._branches.values()]

    def __bool__(self):
        raise InvalidOperation(
            f"Cannot assess truth level of a {self.__class__.__name__} "
            f"using 'bool()' or any operations that rely on it; "
            f"use '&' or '|' for logical and/or comparisons"
        )

    def is_stateful_branch(self, topic_name: Optional[str] = None) -> bool:
        if not topic_name:
            return self._stateful
        return self._branches[topic_name].is_stateful

    def apply(
        self,
        func: Union[DataFrameFunc, DataFrameStatefulFunc],
        stateful: bool = False,
        expand: bool = False,
    ) -> Self:
        """
        Apply a function to transform the value and return a new value.

        The result will be passed downstream as an input value.


        Example Snippet:

        ```python
        # This stores a string in state and capitalizes every column with a string value.
        # A second apply then keeps only the string value columns (shows non-stateful).
        def func(d: dict, state: State):
            value = d["store_field"]
            if value != state.get("my_store_key"):
                state.set("my_store_key") = value
            return {k: v.upper() if isinstance(v, str) else v for k, v in d.items()}

        sdf = StreamingDataframe()
        sdf = sdf.apply(func, stateful=True)
        sdf = sdf.apply(lambda d: {k: v for k,v in d.items() if isinstance(v, str)})

        ```

        :param func: a function to apply
        :param stateful: if `True`, the function will be provided with a second argument
            of type `State` to perform stateful operations.
        :param expand: if True, expand the returned iterable into individual values
            downstream. If returned value is not iterable, `TypeError` will be raised.
            Default - `False`.
        """
        if stateful:
            self._register_store()
            func = _as_stateful(func=func, state_manager=self.state_manager)

        stream = self.stream.add_apply(func, expand=expand)
        return self._clone(stream=stream)

    def update(
        self, func: Union[DataFrameFunc, DataFrameStatefulFunc], stateful: bool = False
    ) -> Self:
        """
        Apply a function to mutate value in-place or to perform a side effect
        that doesn't update the value (e.g. print a value to the console).

        The result of the function will be ignored, and the original value will be
        passed downstream.


        Example Snippet:

        ```python
        # Stores a value and mutates a list by appending a new item to it.
        # Also prints to console.

        def func(values: list, state: State):
            value = values[0]
            if value != state.get("my_store_key"):
                state.set("my_store_key") = value
            values.append("new_item")

        sdf = StreamingDataframe()
        sdf = sdf.update(func, stateful=True)
        sdf = sdf.update(lambda value: print("Received value: ", value))
        ```

        :param func: function to update value
        :param stateful: if `True`, the function will be provided with a second argument
            of type `State` to perform stateful operations.
        """
        if stateful:
            self._register_store()
            func = _as_stateful(func=func, state_manager=self.state_manager)

        stream = self.stream.add_update(func)
        return self._clone(stream=stream)

    def filter(
        self, func: Union[DataFrameFunc, DataFrameStatefulFunc], stateful: bool = False
    ) -> Self:
        """
        Filter value using provided function.

        If the function returns True-like value, the original value will be
        passed downstream.
        Otherwise, the `Filtered` exception will be raised (further processing for that
        message will be skipped).


        Example Snippet:

        ```python
        # Stores a value and allows further processing only if the value is greater than
        # what was previously stored.

        def func(d: dict, state: State):
            value = d["my_value"]
            if value > state.get("my_store_key"):
                state.set("my_store_key") = value
                return True
            return False

        sdf = StreamingDataframe()
        sdf = sdf.filter(func, stateful=True)
        ```


        :param func: function to filter value
        :param stateful: if `True`, the function will be provided with second argument
            of type `State` to perform stateful operations.
        """

        if stateful:
            self._register_store()
            func = _as_stateful(func=func, state_manager=self.state_manager)

        stream = self.stream.add_filter(func)
        return self._clone(stream=stream)

    def group_by(
        self,
        key: Union[str, DataFrameFunc],
        name: Optional[str] = None,
        value_deserializer: Optional[DeserializerType] = "json",
        key_deserializer: Optional[DeserializerType] = "json",
        value_serializer: Optional[SerializerType] = "json",
        key_serializer: Optional[SerializerType] = "json",
    ) -> Self:
        """
        "Groups" messages by re-keying them via the provided group_by operation
        on their message values.

        This enables things like aggregations on messages with non-matching keys.

        You can provide a column name (uses the column's value) or a custom function
        to generate this new key.

        >**NOTE:** group_by generates a topic that copies the original topic's settings.

        Example Snippet:

        ```python
        # We have customer purchase events where the message key is the "store_id",
        # but we want to calculate sales per customer (by "customer_account_id").

        def func(d: dict, state: State):
            current_total = state.get("customer_sum", 0)
            new_total = current_total + d["customer_spent"]
            state.set("customer_sum", new_total)
            d["customer_total"] = new_total
            return d

        sdf = StreamingDataframe()
        sdf = sdf.group_by("customer_account_id")
        sdf = sdf.apply(func, stateful=True)
        ```


        :param key: how the new key should be generated from the message value;
            requires a column name (string) or a callable that takes the message value.
        :param name: a name for the op (must be unique per group-by), required if `key`
            is a custom callable.
        :param value_deserializer: a deserializer type for values; default - JSON
        :param key_deserializer: a deserializer type for keys; default - JSON
        :param value_serializer: a serializer type for values; default - JSON
        :param key_serializer: a serializer type for keys; default - JSON

        :return: a clone with this operation added (assign to keep its effect).
        """
        if isinstance(key, str):
            _gb_key_op = lambda row: row[key]
            name = name or key
        elif callable(key):
            _gb_key_op = lambda row: key(row)
            if not name:
                raise ValueError(
                    "group_by requires 'name' parameter when 'key' is a function"
                )
        else:
            raise TypeError("group_by 'key' must be callable or a string (column name)")

        groupby_topic = self._topic_manager.repartition_topic(
            operation=name,
            consumer_group=self._application._consumer_group,
            topic_name=self._topic.name,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            key_deserializer=key_deserializer,
            value_deserializer=value_deserializer,
        )
        self._finalize_branch(self.to_topic(groupby_topic, key=_gb_key_op))
        return self._clone(topic=groupby_topic, stateful=False)

    @property
    def producer(self) -> RowProducerProto:
        if self._real_producer is None:
            raise RuntimeError("Producer instance has not been provided")
        return self._real_producer

    @producer.setter
    def producer(self, producer: RowProducerProto):
        self._real_producer = producer

    @staticmethod
    def contains(key: str) -> StreamingSeries:
        """
        Check if the key is present in the Row value.


        Example Snippet:

        ```python
        # Add new column 'has_column' which contains a boolean indicating
        # the presence of 'column_x'

        sdf = StreamingDataframe()
        sdf['has_column'] = sdf.contains('column_x')
        ```


        :param key: a column name to check.
        :return: a Column object that evaluates to True if the key is present
            or False otherwise.
        """

        return StreamingSeries.from_func(lambda value: key in value)

    def to_topic(
        self, topic: Topic, key: Optional[Callable[[object], object]] = None
    ) -> Self:
        """
        Produce current value to a topic. You can optionally specify a new key.

        >***NOTE:*** A `RowProducer` instance must be assigned to
        `StreamingDataFrame.producer` if not using :class:`quixstreams.app.Application`
         to facilitate the execution of StreamingDataFrame.


        Example Snippet:

        ```python
        from quixstreams import Application

        # Produce to two different topics, changing the key for one of them.

        app = Application()
        input_topic = app.topic("input_x")
        output_topic_0 = app.topic("output_a")
        output_topic_1 = app.topic("output_b")

        sdf = app.dataframe(input_topic)
        sdf = sdf.to_topic(output_topic_0)
        sdf = sdf.to_topic(output_topic_1, key=lambda data: data["a_field"])
        ```

        :param topic: instance of `Topic`
        :param key: a callable to generate a new message key, optional.
            If passed, the return type of this callable must be serializable
            by `key_serializer` defined for this Topic object.
            By default, the current message key will be used.

        """
        return self.update(
            lambda value: self._produce(topic, value, key=key(value) if key else None)
        )

    def _finalize_branch(self, branch: Self):
        """
        Add a StreamingDataFrame to the branch cache via its corresponding topic name.

        Implies no further operations will be added to that branch.

        :param branch: a StreamingDataFrame instance
        """
        self._branches[self._topic.name] = branch

    def compose(self) -> Dict[str, StreamCallable]:
        """
        Compose all functions of this StreamingDataFrame into one big closure.

        Closures are more performant than calling all the functions in the
        `StreamingDataFrame` one-by-one.

        Generally not required by users; the `quixstreams.app.Application` class will
        do this automatically.


        Example Snippet:

        ```python
        from quixstreams import Application
        sdf = app.dataframe()
        sdf = sdf.apply(apply_func)
        sdf = sdf.filter(filter_func)
        sdf = sdf.compose()

        result_0 = sdf({"my": "record"})
        result_1 = sdf({"other": "record"})
        ```


        :return: a function that accepts "value"
            and returns a result of StreamingDataFrame
        """
        self._finalize_branch(self)
        return {name: sdf.stream.compose() for name, sdf in self._branches.items()}

    def test(
        self,
        value: object,
        ctx: Optional[MessageContext] = None,
        mock_produce: bool = False,
    ) -> Any:
        """
        A shorthand to test `StreamingDataFrame` with provided value
        and `MessageContext`.

        :param value: value to pass through `StreamingDataFrame`
        :param ctx: instance of `MessageContext`, optional.
            Provide it if the StreamingDataFrame instance calls `to_topic()`,
            has stateful functions or functions calling `get_current_key()`.
            Default - `None`.
        :param mock_produce: whether to mock out any to_topic operations. Helpful for
            testing sdf operations downstream of a group_by operation without a valid
            Kafka connection.

        :return: result of `StreamingDataFrame`
        """
        stack = contextlib.ExitStack()
        if mock_produce:
            stack.enter_context(patch.object(self, "_real_producer"))
            if not ctx:
                ctx = MessageContext(
                    topic="test_topic",
                    partition=0,
                    offset=10,
                    key="test_key",
                    size=100,
                    timestamp=MessageTimestamp(1234567890, TimestampType(1)),
                )
        context = contextvars.copy_context()
        context.run(set_message_context, ctx)
        try:
            composed = self.compose()
            for k in composed:
                value = context.run(composed[k], value)
        finally:
            stack.close()
        return value

    def tumbling_window(
        self,
        duration_ms: Union[int, timedelta],
        grace_ms: Union[int, timedelta] = 0,
        name: Optional[str] = None,
    ) -> TumblingWindowDefinition:
        """
        Create a tumbling window transformation on this StreamingDataFrame.
        Tumbling windows divide time into fixed-sized, non-overlapping windows.

        They allow to perform stateful aggregations like `sum`, `reduce`, etc.
        on top of the data and emit results downstream.

        Notes:

        - Every window is grouped by the current Kafka message key.
        - Messages with `None` key will be ignored.
        - The time windows always use the current event time.



        Example Snippet:

        ```python
        app = Application()
        sdf = app.dataframe(...)

        sdf = (
            # Define a tumbling window of 60s and grace period of 10s
            sdf.tumbling_window(
                duration_ms=timedelta(seconds=60), grace_ms=timedelta(seconds=10.0)
            )

            # Specify the aggregation function
            .sum()

            # Specify how the results should be emitted downstream.
            # "all()" will emit results as they come for each updated window,
            # possibly producing multiple messages per key-window pair
            # "final()" will emit windows only when they are closed and cannot
            # receive any updates anymore.
            .all()
        )
        ```

        :param duration_ms: The length of each window.
            Can be specified as either an `int` representing milliseconds or a
            `timedelta` object.
            >***NOTE:*** `timedelta` objects will be rounded to the closest millisecond
            value.

        :param grace_ms: The grace period for data arrival.
            It allows late-arriving data (data arriving after the window
            has theoretically closed) to be included in the window.
            Can be specified as either an `int` representing milliseconds
            or as a `timedelta` object.
            >***NOTE:*** `timedelta` objects will be rounded to the closest millisecond
            value.

        :param name: The unique identifier for the window. If not provided, it will be
            automatically generated based on the window's properties.

        :return: `TumblingWindowDefinition` instance representing the tumbling window
            configuration.
            This object can be further configured with aggregation functions
            like `sum`, `count`, etc. applied to the StreamingDataFrame.

        """
        self._stateful = True
        duration_ms = ensure_milliseconds(duration_ms)
        grace_ms = ensure_milliseconds(grace_ms)

        return TumblingWindowDefinition(
            duration_ms=duration_ms, grace_ms=grace_ms, dataframe=self, name=name
        )

    def hopping_window(
        self,
        duration_ms: Union[int, timedelta],
        step_ms: Union[int, timedelta],
        grace_ms: Union[int, timedelta] = 0,
        name: Optional[str] = None,
    ) -> HoppingWindowDefinition:
        """
        Create a hopping window transformation on this StreamingDataFrame.
        Hopping windows divide the data stream into overlapping windows based on time.
        The overlap is controlled by the `step_ms` parameter.

        They allow to perform stateful aggregations like `sum`, `reduce`, etc.
        on top of the data and emit results downstream.

        Notes:

        - Every window is grouped by the current Kafka message key.
        - Messages with `None` key will be ignored.
        - The time windows always use the current event time.


        Example Snippet:

        ```python
        app = Application()
        sdf = app.dataframe(...)

        sdf = (
            # Define a hopping window of 60s with step 30s and grace period of 10s
            sdf.hopping_window(
                duration_ms=timedelta(seconds=60),
                step_ms=timedelta(seconds=30),
                grace_ms=timedelta(seconds=10)
            )

            # Specify the aggregation function
            .sum()

            # Specify how the results should be emitted downstream.
            # "all()" will emit results as they come for each updated window,
            # possibly producing multiple messages per key-window pair
            # "final()" will emit windows only when they are closed and cannot
            # receive any updates anymore.
            .all()
        )
        ```

        :param duration_ms: The length of each window. It defines the time span for
            which each window aggregates data.
            Can be specified as either an `int` representing milliseconds
            or a `timedelta` object.
            >***NOTE:*** `timedelta` objects will be rounded to the closest millisecond
            value.

        :param step_ms: The step size for the window.
            It determines how much each successive window moves forward in time.
            Can be specified as either an `int` representing milliseconds
            or a `timedelta` object.
            >***NOTE:*** `timedelta` objects will be rounded to the closest millisecond
            value.

        :param grace_ms: The grace period for data arrival.
            It allows late-arriving data to be included in the window,
            even if it arrives after the window has theoretically moved forward.
            Can be specified as either an `int` representing milliseconds
            or a `timedelta` object.
            >***NOTE:*** `timedelta` objects will be rounded to the closest millisecond
            value.

        :param name: The unique identifier for the window. If not provided, it will be
            automatically generated based on the window's properties.

        :return: `HoppingWindowDefinition` instance representing the hopping
            window configuration.
            This object can be further configured with aggregation functions
            like `sum`, `count`, etc. and applied to the StreamingDataFrame.
        """
        self._stateful = True
        duration_ms = ensure_milliseconds(duration_ms)
        step_ms = ensure_milliseconds(step_ms)
        grace_ms = ensure_milliseconds(grace_ms)

        return HoppingWindowDefinition(
            duration_ms=duration_ms,
            grace_ms=grace_ms,
            step_ms=step_ms,
            dataframe=self,
            name=name,
        )

    def _clone(
        self,
        stream: Optional[Stream] = None,
        topic: Optional[Topic] = None,
        stateful: Optional[bool] = None,
    ) -> Self:
        if stateful is None:
            stateful = self._stateful
        clone = self.__class__(
            stream=stream,
            topic=topic or self._topic,
            application=self._application,
            branches=self._branches,
            stateful=stateful,
        )
        if self._real_producer is not None:
            clone.producer = self._real_producer
        return clone

    def _produce(self, topic: Topic, value: object, key: Optional[object] = None):
        ctx = message_context()
        key = key or ctx.key
        row = Row(value=value, context=ctx)  # noqa
        self.producer.produce_row(row, topic, key=key)

    def _register_store(self):
        """
        Register the default store for input topic in StateStoreManager
        """
        self._stateful = True
        self.state_manager.register_store(topic_name=self._topic.name)

    def __setitem__(self, key, value: Union[Self, object]):
        if isinstance(value, self.__class__):
            diff = self.stream.diff(value.stream)
            diff_composed = diff.compose(
                allow_filters=False, allow_updates=False, allow_expands=False
            )
            stream = self.stream.add_update(
                lambda v: operator.setitem(v, key, diff_composed(v))
            )
        elif isinstance(value, StreamingSeries):
            value_composed = value.compose(allow_filters=False, allow_updates=False)
            stream = self.stream.add_update(
                lambda v: operator.setitem(v, key, value_composed(v))
            )
        else:
            stream = self.stream.add_update(lambda v: operator.setitem(v, key, value))
        self._stream = stream

    @overload
    def __getitem__(self, item: str) -> StreamingSeries: ...

    @overload
    def __getitem__(self, item: Union[StreamingSeries, List[str], Self]) -> Self: ...

    def __getitem__(
        self, item: Union[str, List[str], StreamingSeries, Self]
    ) -> Union[Self, StreamingSeries]:
        if isinstance(item, StreamingSeries):
            # Filter SDF based on StreamingSeries
            item_composed = item.compose(allow_filters=False, allow_updates=False)
            return self.filter(lambda v: item_composed(v))
        elif isinstance(item, self.__class__):
            # Filter SDF based on another SDF
            diff = self.stream.diff(item.stream)
            diff_composed = diff.compose(
                allow_filters=False, allow_updates=False, allow_expands=False
            )
            return self.filter(lambda v: diff_composed(v))
        elif isinstance(item, list):
            # Take only certain keys from the dict and return a new dict
            return self.apply(lambda v: {k: v[k] for k in item})
        elif isinstance(item, str):
            # Create a StreamingSeries based on key
            return StreamingSeries(name=item)
        else:
            raise TypeError(f'Unsupported key type "{type(item)}"')


def _as_stateful(
    func: DataFrameStatefulFunc, state_manager: StateStoreManager
) -> DataFrameFunc:
    @functools.wraps(func)
    def wrapper(value: object) -> object:
        transaction = state_manager.get_store_transaction()
        key = message_key()
        # Prefix all the state keys by the message key
        with transaction.with_prefix(prefix=key):
            # Pass a State object with an interface limited to the key updates only
            return func(value, transaction.state)

    return wrapper
