from __future__ import annotations

import contextvars
import functools
import operator
import pprint
from datetime import timedelta
from typing import (
    Any,
    Callable,
    Collection,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
    cast,
    overload,
)

from typing_extensions import Self

from quixstreams.context import (
    message_context,
    set_message_context,
)
from quixstreams.core.stream import (
    ApplyCallback,
    ApplyWithMetadataCallback,
    FilterCallback,
    FilterWithMetadataCallback,
    Stream,
    UpdateCallback,
    UpdateWithMetadataCallback,
    VoidExecutor,
)
from quixstreams.models import (
    HeaderValue,
    MessageContext,
    Row,
    Topic,
    TopicManager,
)
from quixstreams.models.serializers import DeserializerType, SerializerType
from quixstreams.processing import ProcessingContext
from quixstreams.sinks import BaseSink
from quixstreams.state.base import State

from .base import BaseStreaming
from .exceptions import InvalidOperation
from .registry import DataframeRegistry
from .series import StreamingSeries
from .utils import ensure_milliseconds
from .windows import (
    HoppingWindowDefinition,
    SlidingWindowDefinition,
    TumblingWindowDefinition,
)

ApplyCallbackStateful = Callable[[Any, State], Any]
ApplyWithMetadataCallbackStateful = Callable[[Any, Any, int, Any, State], Any]
UpdateCallbackStateful = Callable[[Any, State], None]
UpdateWithMetadataCallbackStateful = Callable[[Any, Any, int, Any, State], None]
FilterCallbackStateful = Callable[[Any, State], bool]
FilterWithMetadataCallbackStateful = Callable[[Any, Any, int, Any, State], bool]


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
    sdf = StreamingDataFrame()
    sdf = sdf.apply(a_func)
    sdf = sdf.filter(another_func)
    sdf = sdf.to_topic(topic_obj)
    ```
    """

    def __init__(
        self,
        topic: Topic,
        topic_manager: TopicManager,
        registry: DataframeRegistry,
        processing_context: ProcessingContext,
        stream: Optional[Stream] = None,
    ):
        self._stream: Stream = stream or Stream()
        self._topic = topic
        self._topic_manager = topic_manager
        self._registry = registry
        self._processing_context = processing_context
        self._producer = processing_context.producer
        self._locked = False

    @property
    def processing_context(self) -> ProcessingContext:
        return self._processing_context

    @property
    def stream(self) -> Stream:
        return self._stream

    @property
    def topic(self) -> Topic:
        return self._topic

    @overload
    def apply(self, func: ApplyCallback, *, expand: bool = ...) -> Self: ...

    @overload
    def apply(
        self,
        func: ApplyWithMetadataCallback,
        *,
        metadata: Literal[True],
        expand: bool = ...,
    ) -> Self: ...

    @overload
    def apply(
        self,
        func: ApplyCallbackStateful,
        *,
        stateful: Literal[True],
        expand: bool = ...,
    ) -> Self: ...

    @overload
    def apply(
        self,
        func: ApplyWithMetadataCallbackStateful,
        *,
        stateful: Literal[True],
        metadata: Literal[True],
        expand: bool = ...,
    ) -> Self: ...

    def apply(
        self,
        func: Union[
            ApplyCallback,
            ApplyCallbackStateful,
            ApplyWithMetadataCallback,
            ApplyWithMetadataCallbackStateful,
        ],
        *,
        stateful: bool = False,
        expand: bool = False,
        metadata: bool = False,
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

        sdf = StreamingDataFrame()
        sdf = sdf.apply(func, stateful=True)
        sdf = sdf.apply(lambda d: {k: v for k,v in d.items() if isinstance(v, str)})

        ```

        :param func: a function to apply
        :param stateful: if `True`, the function will be provided with a second argument
            of type `State` to perform stateful operations.
        :param expand: if True, expand the returned iterable into individual values
            downstream. If returned value is not iterable, `TypeError` will be raised.
            Default - `False`.
        :param metadata: if True, the callback will receive key, timestamp and headers
            along with the value.
            Default - `False`.
        """
        if stateful:
            self._register_store()
            # Force the callback to accept metadata
            with_metadata_func = (
                cast(ApplyWithMetadataCallbackStateful, func)
                if metadata
                else _as_metadata_func(cast(ApplyCallbackStateful, func))
            )
            stateful_func = _as_stateful(
                func=with_metadata_func,
                processing_context=self._processing_context,
            )
            stream = self.stream.add_apply(stateful_func, expand=expand, metadata=True)
        else:
            stream = self.stream.add_apply(
                cast(Union[ApplyCallback, ApplyWithMetadataCallback], func),
                expand=expand,
                metadata=metadata,
            )
        return self.__dataframe_clone__(stream=stream)

    @overload
    def update(self, func: UpdateCallback) -> Self: ...

    @overload
    def update(
        self, func: UpdateWithMetadataCallback, *, metadata: Literal[True]
    ) -> Self: ...

    @overload
    def update(
        self, func: UpdateCallbackStateful, *, stateful: Literal[True]
    ) -> Self: ...

    @overload
    def update(
        self,
        func: UpdateWithMetadataCallbackStateful,
        *,
        stateful: Literal[True],
        metadata: Literal[True],
    ) -> Self: ...

    def update(
        self,
        func: Union[
            UpdateCallback,
            UpdateCallbackStateful,
            UpdateWithMetadataCallback,
            UpdateWithMetadataCallbackStateful,
        ],
        *,
        stateful: bool = False,
        metadata: bool = False,
    ) -> Self:
        """
        Apply a function to mutate value in-place or to perform a side effect
        (e.g., printing a value to the console).

        The result of the function will be ignored, and the original value will be
        passed downstream.

        This operation occurs in-place, meaning reassignment is entirely OPTIONAL: the
        original `StreamingDataFrame` is returned for chaining (`sdf.update().print()`).


        Example Snippet:

        ```python
        # Stores a value and mutates a list by appending a new item to it.
        # Also prints to console.

        def func(values: list, state: State):
            value = values[0]
            if value != state.get("my_store_key"):
                state.set("my_store_key") = value
            values.append("new_item")

        sdf = StreamingDataFrame()
        sdf = sdf.update(func, stateful=True)
        # does not require reassigning
        sdf.update(lambda v: v.append(1))
        ```

        :param func: function to update value
        :param stateful: if `True`, the function will be provided with a second argument
            of type `State` to perform stateful operations.
        :param metadata: if True, the callback will receive key, timestamp and headers
            along with the value.
            Default - `False`.
        :return: the updated StreamingDataFrame instance (reassignment NOT required).
        """
        if stateful:
            self._register_store()
            # Force the callback to accept metadata
            with_metadata_func = (
                func
                if metadata
                else _as_metadata_func(cast(UpdateCallbackStateful, func))
            )
            stateful_func = _as_stateful(
                func=cast(UpdateWithMetadataCallbackStateful, with_metadata_func),
                processing_context=self._processing_context,
            )
            return self._add_update(
                cast(UpdateWithMetadataCallback, stateful_func), metadata=True
            )
        else:
            return self._add_update(
                cast(Union[UpdateCallback, UpdateWithMetadataCallback], func),
                metadata=metadata,
            )

    @overload
    def filter(self, func: FilterCallback) -> Self: ...

    @overload
    def filter(
        self, func: FilterWithMetadataCallback, *, metadata: Literal[True]
    ) -> Self: ...

    @overload
    def filter(
        self, func: FilterCallbackStateful, *, stateful: Literal[True]
    ) -> Self: ...

    @overload
    def filter(
        self,
        func: FilterWithMetadataCallbackStateful,
        *,
        stateful: Literal[True],
        metadata: Literal[True],
    ) -> Self: ...

    def filter(
        self,
        func: Union[
            FilterCallback,
            FilterCallbackStateful,
            FilterWithMetadataCallback,
            FilterWithMetadataCallbackStateful,
        ],
        *,
        stateful: bool = False,
        metadata: bool = False,
    ) -> Self:
        """
        Filter value using provided function.

        If the function returns True-like value, the original value will be
        passed downstream.

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

        sdf = StreamingDataFrame()
        sdf = sdf.filter(func, stateful=True)
        ```


        :param func: function to filter value
        :param stateful: if `True`, the function will be provided with second argument
            of type `State` to perform stateful operations.
        :param metadata: if True, the callback will receive key, timestamp and headers
            along with the value.
            Default - `False`.
        """

        if stateful:
            self._register_store()
            # Force the callback to accept metadata
            with_metadata_func = (
                func
                if metadata
                else _as_metadata_func(cast(FilterCallbackStateful, func))
            )
            stateful_func = _as_stateful(
                func=cast(FilterWithMetadataCallbackStateful, with_metadata_func),
                processing_context=self._processing_context,
            )
            stream = self.stream.add_filter(stateful_func, metadata=True)
        else:
            stream = self.stream.add_filter(
                cast(Union[FilterCallback, FilterWithMetadataCallback], func),
                metadata=metadata,
            )
        return self.__dataframe_clone__(stream=stream)

    @overload
    def group_by(
        self,
        key: str,
        name: Optional[str] = ...,
        value_deserializer: Optional[DeserializerType] = ...,
        key_deserializer: Optional[DeserializerType] = ...,
        value_serializer: Optional[SerializerType] = ...,
        key_serializer: Optional[SerializerType] = ...,
    ) -> Self: ...

    @overload
    def group_by(
        self,
        key: Callable[[Any], Any],
        name: str,
        value_deserializer: Optional[DeserializerType] = ...,
        key_deserializer: Optional[DeserializerType] = ...,
        value_serializer: Optional[SerializerType] = ...,
        key_serializer: Optional[SerializerType] = ...,
    ) -> Self: ...

    def group_by(
        self,
        key: Union[str, Callable[[Any], Any]],
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

        `.groupby()` can only be performed once per `StreamingDataFrame` instance.

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

        sdf = StreamingDataFrame()
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
        if not key:
            raise ValueError('Parameter "key" cannot be empty')
        if callable(key) and not name:
            raise ValueError(
                'group_by requires "name" parameter when "key" is a function'
            )

        groupby_topic = self._topic_manager.repartition_topic(
            operation=name or key,
            topic_name=self._topic.name,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            key_deserializer=key_deserializer,
            value_deserializer=value_deserializer,
        )

        self.to_topic(topic=groupby_topic, key=self._groupby_key(key))
        groupby_sdf = self.__dataframe_clone__(topic=groupby_topic)
        self._registry.register_groupby(source_sdf=self, new_sdf=groupby_sdf)
        return groupby_sdf

    def contains(self, key: str) -> StreamingSeries:
        """
        Check if the key is present in the Row value.


        Example Snippet:

        ```python
        # Add new column 'has_column' which contains a boolean indicating
        # the presence of 'column_x'

        sdf = StreamingDataFrame()
        sdf['has_column'] = sdf.contains('column_x')
        ```


        :param key: a column name to check.
        :return: a Column object that evaluates to True if the key is present
            or False otherwise.
        """

        return StreamingSeries.from_apply_callback(
            lambda value, key_, timestamp, headers: key in value, sdf_id=id(self)
        )

    def to_topic(
        self, topic: Topic, key: Optional[Callable[[Any], Any]] = None
    ) -> Self:
        """
        Produce current value to a topic. You can optionally specify a new key.

        This operation occurs in-place, meaning reassignment is entirely OPTIONAL: the
        original `StreamingDataFrame` is returned for chaining (`sdf.update().print()`).

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
        # does not require reassigning
        sdf.to_topic(output_topic_1, key=lambda data: data["a_field"])
        ```

        :param topic: instance of `Topic`
        :param key: a callable to generate a new message key, optional.
            If passed, the return type of this callable must be serializable
            by `key_serializer` defined for this Topic object.
            By default, the current message key will be used.
        :return: the updated StreamingDataFrame instance (reassignment NOT required).
        """
        return self._add_update(
            lambda value, orig_key, timestamp, headers: self._produce(
                topic=topic,
                value=value,
                key=orig_key if key is None else key(value),
                timestamp=timestamp,
                headers=headers,
            ),
            metadata=True,
        )

    def set_timestamp(self, func: Callable[[Any, Any, int, Any], int]) -> Self:
        """
        Set a new timestamp based on the current message value and its metadata.

        The new timestamp will be used in windowed aggregations and when producing
        messages to the output topics.

        The new timestamp must be in milliseconds to conform Kafka requirements.

        Example Snippet:

        ```python
        from quixstreams import Application


        app = Application()
        input_topic = app.topic("data")

        sdf = app.dataframe(input_topic)
        # Updating the record's timestamp based on the value
        sdf = sdf.set_timestamp(lambda value, key, timestamp, headers: value['new_timestamp'])
        ```

        :param func: callable accepting the current value, key, timestamp, and headers.
            It's expected to return a new timestamp as integer in milliseconds.
        :return: a new StreamingDataFrame instance
        """

        @functools.wraps(func)
        def _set_timestamp_callback(
            value: Any,
            key: Any,
            timestamp: int,
            headers: Any,
        ) -> Tuple[Any, Any, int, Any]:
            new_timestamp = func(value, key, timestamp, headers)
            return value, key, new_timestamp, headers

        stream = self.stream.add_transform(func=_set_timestamp_callback)
        return self.__dataframe_clone__(stream=stream)

    def set_headers(
        self,
        func: Callable[
            [Any, Any, int, List[Tuple[str, HeaderValue]]],
            Collection[Tuple[str, HeaderValue]],
        ],
    ) -> Self:
        """
        Set new message headers based on the current message value and metadata.

        The new headers will be used when producing messages to the output topics.

        The provided callback must accept value, key, timestamp, and headers,
        and return a new collection of (header, value) tuples.

        Example Snippet:

        ```python
        from quixstreams import Application


        app = Application()
        input_topic = app.topic("data")

        sdf = app.dataframe(input_topic)
        # Updating the record's headers based on the value and metadata
        sdf = sdf.set_headers(lambda value, key, timestamp, headers: [('id', value['id'])])
        ```

        :param func: callable accepting the current value, key, timestamp, and headers.
            It's expected to return a new set of headers
            as a collection of (header, value) tuples.
        :return: a new StreamingDataFrame instance
        """

        @functools.wraps(func)
        def _set_headers_callback(
            value: Any,
            key: Any,
            timestamp: int,
            headers: Collection[Tuple[str, HeaderValue]],
        ) -> Tuple[Any, Any, int, Collection[Tuple[str, HeaderValue]]]:
            # Create a shallow copy of original headers to prevent potential mutations
            # of the same collection
            headers = list(headers) if headers else []
            new_headers = func(value, key, timestamp, headers)
            return value, key, timestamp, new_headers

        stream = self.stream.add_transform(func=_set_headers_callback)
        return self.__dataframe_clone__(stream=stream)

    def print(self, pretty: bool = True, metadata: bool = False) -> Self:
        """
        Print out the current message value (and optionally, the message metadata) to
        stdout (console) (like the built-in `print` function).

        Can also output a more dict-friendly format with `pretty=True`.

        This operation occurs in-place, meaning reassignment is entirely OPTIONAL: the
        original `StreamingDataFrame` is returned for chaining (`sdf.update().print()`).

        > NOTE: prints the current (edited) values, not the original values.

        Example Snippet:

        ```python
        from quixstreams import Application


        app = Application()
        input_topic = app.topic("data")

        sdf = app.dataframe(input_topic)
        sdf["edited_col"] = sdf["orig_col"] + "edited"
        # print the updated message value with the newly added column
        sdf.print()
        ```

        :param pretty: Whether to use "pprint" formatting, which uses new-lines and
            indents for easier console reading (but might be worse for log parsing).
        :param metadata: Whether to additionally print the key, timestamp, and headers
        :return: the updated StreamingDataFrame instance (reassignment NOT required).
        """
        print_args = ["value", "key", "timestamp", "headers"]
        if pretty:
            printer = functools.partial(pprint.pprint, indent=2, sort_dicts=False)
        else:
            printer = print
        return self._add_update(
            lambda *args: printer({print_args[i]: args[i] for i in range(len(args))}),
            metadata=metadata,
        )

    def compose(
        self,
        sink: Optional[Callable[[Any, Any, int, Any], None]] = None,
    ) -> Dict[str, VoidExecutor]:
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

        :param sink: callable to accumulate the results of the execution, optional.
        :return: a function that accepts "value"
            and returns a result of StreamingDataFrame
        """
        return self._registry.compose_all(sink)

    def test(
        self,
        value: Any,
        key: Any,
        timestamp: int,
        headers: Optional[Any] = None,
        ctx: Optional[MessageContext] = None,
        topic: Optional[Topic] = None,
    ) -> List[Any]:
        """
        A shorthand to test `StreamingDataFrame` with provided value
        and `MessageContext`.

        :param value: value to pass through `StreamingDataFrame`
        :param key: key to pass through `StreamingDataFrame`
        :param timestamp: timestamp to pass through `StreamingDataFrame`
        :param ctx: instance of `MessageContext`, optional.
            Provide it if the StreamingDataFrame instance calls `to_topic()`,
            has stateful functions or windows.
            Default - `None`.
        :param topic: optionally, a topic branch to test with

        :return: result of `StreamingDataFrame`
        """
        if not topic:
            topic = self._topic
        context = contextvars.copy_context()
        context.run(set_message_context, ctx)
        result = []
        composed = self.compose(
            sink=lambda value_, key_, timestamp_, headers_: result.append(
                (value_, key_, timestamp_, headers_)
            )
        )
        context.run(composed[topic.name], value, key, timestamp, headers)
        return result

    def tumbling_window(
        self,
        duration_ms: Union[int, timedelta],
        grace_ms: Union[int, timedelta] = 0,
        name: Optional[str] = None,
    ) -> TumblingWindowDefinition:
        """
        Create a tumbling window transformation on this StreamingDataFrame.
        Tumbling windows divide time into fixed-sized, non-overlapping windows.

        They allow performing stateful aggregations like `sum`, `reduce`, etc.
        on top of the data and emit results downstream.

        Notes:

        - The timestamp of the aggregation result is set to the window start timestamp.
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
            # "current()" will emit results as they come for each updated window,
            # possibly producing multiple messages per key-window pair
            # "final()" will emit windows only when they are closed and cannot
            # receive any updates anymore.
            .current()
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

        They allow performing stateful aggregations like `sum`, `reduce`, etc.
        on top of the data and emit results downstream.

        Notes:

        - The timestamp of the aggregation result is set to the window start timestamp.
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
            # "current()" will emit results as they come for each updated window,
            # possibly producing multiple messages per key-window pair
            # "final()" will emit windows only when they are closed and cannot
            # receive any updates anymore.
            .current()
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

    def sliding_window(
        self,
        duration_ms: Union[int, timedelta],
        grace_ms: Union[int, timedelta] = 0,
        name: Optional[str] = None,
    ) -> SlidingWindowDefinition:
        """
        Create a sliding window transformation on this StreamingDataFrame.
        Sliding windows continuously evaluate the stream with a fixed step of 1 ms
        allowing for overlapping, but not redundant windows of a fixed size.

        Sliding windows are similar to hopping windows with step_ms set to 1,
        but are siginificantly more perforant.

        They allow performing stateful aggregations like `sum`, `reduce`, etc.
        on top of the data and emit results downstream.

        Notes:

        - The timestamp of the aggregation result is set to the window start timestamp.
        - Every window is grouped by the current Kafka message key.
        - Messages with `None` key will be ignored.
        - The time windows always use the current event time.
        - Windows are inclusive on both the start end end time.
        - Every window contains a distinct aggregation.

        Example Snippet:

        ```python
        app = Application()
        sdf = app.dataframe(...)

        sdf = (
            # Define a sliding window of 60s with a grace period of 10s
            sdf.sliding_window(
                duration_ms=timedelta(seconds=60),
                grace_ms=timedelta(seconds=10)
            )

            # Specify the aggregation function
            .sum()

            # Specify how the results should be emitted downstream.
            # "current()" will emit results as they come for each updated window,
            # possibly producing multiple messages per key-window pair
            # "final()" will emit windows only when they are closed and cannot
            # receive any updates anymore.
            .current()
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

        :return: `SlidingWindowDefinition` instance representing the sliding window
            configuration.
            This object can be further configured with aggregation functions
            like `sum`, `count`, etc. applied to the StreamingDataFrame.
        """

        duration_ms = ensure_milliseconds(duration_ms)
        grace_ms = ensure_milliseconds(grace_ms)

        return SlidingWindowDefinition(
            duration_ms=duration_ms, grace_ms=grace_ms, dataframe=self, name=name
        )

    def drop(
        self,
        columns: Union[str, List[str]],
        errors: Literal["ignore", "raise"] = "raise",
    ) -> Self:
        """
        Drop column(s) from the message value (value must support `del`, like a dict).

        This operation occurs in-place, meaning reassignment is entirely OPTIONAL: the
        original `StreamingDataFrame` is returned for chaining (`sdf.update().print()`).


        Example Snippet:

        ```python
        # Remove columns "x" and "y" from the value.
        # This would transform {"x": 1, "y": 2, "z": 3} to {"z": 3}

        sdf = StreamingDataFrame()
        sdf.drop(["x", "y"])
        ```

        :param columns: a single column name or a list of names, where names are `str`
        :param errors: If "ignore", suppress error and only existing labels are dropped.
            Default - `"raise"`.

        :return: a new StreamingDataFrame instance
        """
        if isinstance(columns, list):
            if not columns:
                return self
            if not all(isinstance(s, str) for s in columns):
                raise TypeError("column list must contain strings only")
        elif isinstance(columns, str):
            columns = [columns]
        else:
            raise TypeError(
                f"Expected a string or a list of strings, not {type(columns)}"
            )
        return self._add_update(
            lambda value: _drop(value, columns, ignore_missing=errors == "ignore"),
            metadata=False,
        )

    def sink(self, sink: BaseSink):
        """
        Sink the processed data to the specified destination.

        Internally, each processed record is added to a sink, and the sinks are
        flushed on each checkpoint.
        The offset will be committed only if all the sinks for all topic partitions
        are flushed successfully.

        Additionally, Sinks may signal the backpressure to the application
        (e.g., when the destination is rate-limited).
        When this happens, the application will pause the corresponding topic partition
        and resume again after the timeout.
        The backpressure handling and timeouts are defined by the specific sinks.

        Note: `sink()` is a terminal operation - it cannot receive any additional
        operations, but branches can still be generated from its originating SDF.

        """
        self._processing_context.sink_manager.register(sink)

        def _sink_callback(
            value: Any, key: Any, timestamp: int, headers: List[Tuple[str, HeaderValue]]
        ):
            ctx = message_context()
            sink.add(
                value=value,
                key=key,
                timestamp=timestamp,
                headers=headers,
                partition=ctx.partition,
                topic=ctx.topic,
                offset=ctx.offset,
            )

        # uses apply without returning to make this operation terminal
        self.apply(_sink_callback, metadata=True)

    def _produce(
        self,
        topic: Topic,
        value: object,
        key: Any,
        timestamp: int,
        headers: Any,
    ):
        ctx = message_context()
        row = Row(
            value=value, key=key, timestamp=timestamp, context=ctx, headers=headers
        )
        self._producer.produce_row(row=row, topic=topic, key=key, timestamp=timestamp)

    def _add_update(
        self,
        func: Union[UpdateCallback, UpdateWithMetadataCallback],
        metadata: bool = False,
    ):
        self._stream = self._stream.add_update(func, metadata=metadata)
        return self

    def _register_store(self):
        """
        Register the default store for input topic in StateStoreManager
        """
        self._processing_context.state_manager.register_store(
            topic_name=self._topic.name
        )

    def _groupby_key(
        self, key: Union[str, Callable[[Any], Any]]
    ) -> Callable[[Any], Any]:
        """
        Generate the underlying groupby key function based on users provided input.
        """
        if isinstance(key, str):
            return lambda row: row[key]
        elif callable(key):
            return lambda row: key(row)
        else:
            raise TypeError("group_by 'key' must be callable or string (column name)")

    def __dataframe_clone__(
        self,
        stream: Optional[Stream] = None,
        topic: Optional[Topic] = None,
    ) -> Self:
        """
        Clone the StreamingDataFrame with a new `stream` and `topic` parameters.

        :param stream: instance of `Stream`, optional.
        :param topic: instance of `Topic`, optional.
        :return: a new `StreamingDataFrame`.
        """
        clone = self.__class__(
            stream=stream,
            topic=topic or self._topic,
            processing_context=self._processing_context,
            topic_manager=self._topic_manager,
            registry=self._registry,
        )
        return clone

    def __setitem__(self, item_key: Any, item: Union[Self, object]):
        if isinstance(item, self.__class__):
            # Update an item key with a result of another sdf.apply()
            diff = self.stream.diff(item.stream)
            other_sdf_composed = diff.compose_returning()
            self._add_update(
                lambda value, key, timestamp, headers: operator.setitem(
                    value,
                    item_key,
                    other_sdf_composed(value, key, timestamp, headers)[0],
                ),
                metadata=True,
            )
        elif isinstance(item, StreamingSeries):
            # Update an item key with a result of another series
            if id(self) != item.sdf_id:
                raise InvalidOperation(
                    "Column-setting operations must originate from target SDF; "
                    'ex: `sdf1["x"] = sdf1["y"] + 1`, NOT `sdf1["x"] = sdf2["y"] + 1` '
                )

            series_composed = item.compose_returning()
            self._add_update(
                lambda value, key, timestamp, headers: operator.setitem(
                    value, item_key, series_composed(value, key, timestamp, headers)[0]
                ),
                metadata=True,
            )
        else:
            # Update an item key with a constant
            self._add_update(lambda value: operator.setitem(value, item_key, item))

    @overload
    def __getitem__(self, item: str) -> StreamingSeries: ...

    @overload
    def __getitem__(self, item: Union[StreamingSeries, List[str], Self]) -> Self: ...

    def __getitem__(
        self, item: Union[str, List[str], StreamingSeries, Self]
    ) -> Union[Self, StreamingSeries]:
        if isinstance(item, StreamingSeries):
            # Filter SDF based on StreamingSeries
            series_composed = item.compose_returning()
            return self.filter(
                lambda value, key, timestamp, headers: series_composed(
                    value, key, timestamp, headers
                )[0],
                metadata=True,
            )
        elif isinstance(item, self.__class__):
            diff = self.stream.diff(item.stream)
            other_sdf_composed = diff.compose_returning()
            return self.filter(
                lambda value, key, timestamp, headers: other_sdf_composed(
                    value, key, timestamp, headers
                )[0],
                metadata=True,
            )
        elif isinstance(item, list):
            # Make a projection and filter keys from the dict
            return self.apply(lambda value: {k: value[k] for k in item})
        elif isinstance(item, str):
            # Create a StreamingSeries based on a column name
            return StreamingSeries(name=item, sdf_id=id(self))
        else:
            raise TypeError(f'Unsupported key type "{type(item)}"')

    def __bool__(self):
        raise InvalidOperation(
            f"Cannot assess truth level of a {self.__class__.__name__} "
            f"using 'bool()' or any operations that rely on it; "
            f"use '&' or '|' for logical and/or comparisons"
        )


def _drop(value: Dict, columns: List[str], ignore_missing: bool = False):
    """
    remove columns from the value, inplace
    :param value: a dict or something that supports `del`
    :param columns: a list of column names
    :param ignore_missing: if True, ignore missing columns
    """
    for column in columns:
        try:
            del value[column]
        except KeyError:
            if not ignore_missing:
                raise


def _as_metadata_func(
    func: Union[ApplyCallbackStateful, FilterCallbackStateful, UpdateCallbackStateful],
) -> Union[
    ApplyWithMetadataCallbackStateful,
    FilterWithMetadataCallbackStateful,
    UpdateWithMetadataCallbackStateful,
]:
    @functools.wraps(func)
    def wrapper(
        value: Any, _key: Any, _timestamp: int, _headers: Any, state: State
    ) -> Any:
        return func(value, state)

    return wrapper


def _as_stateful(
    func: Union[
        ApplyWithMetadataCallbackStateful,
        FilterWithMetadataCallbackStateful,
        UpdateWithMetadataCallbackStateful,
    ],
    processing_context: ProcessingContext,
) -> Union[
    ApplyWithMetadataCallback,
    FilterWithMetadataCallback,
    UpdateWithMetadataCallback,
]:
    @functools.wraps(func)
    def wrapper(value: Any, key: Any, timestamp: int, headers: Any) -> Any:
        ctx = message_context()
        transaction = processing_context.checkpoint.get_store_transaction(
            topic=ctx.topic, partition=ctx.partition
        )
        # Pass a State object with an interface limited to the key updates only
        # and prefix all the state keys by the message key
        state = transaction.as_state(prefix=key)
        return func(value, key, timestamp, headers, state)

    return wrapper
