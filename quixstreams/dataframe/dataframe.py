from __future__ import annotations

import contextvars
import functools
import itertools
import operator
import pprint
import typing
import warnings
from datetime import timedelta
from operator import attrgetter
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    TypeVar,
    Union,
    cast,
    overload,
)

from quixstreams.context import (
    message_context,
    set_message_context,
)
from quixstreams.core.stream import (
    ApplyCallback,
    ApplyExpandedCallback,
    ApplyWithMetadataCallback,
    ApplyWithMetadataExpandedCallback,
    FilterCallback,
    FilterWithMetadataCallback,
    Stream,
    UpdateCallback,
    UpdateWithMetadataCallback,
    VoidExecutor,
)
from quixstreams.models import (
    HeadersTuples,
    MessageContext,
    Row,
    Topic,
    TopicManager,
)
from quixstreams.models.serializers import DeserializerType, SerializerType
from quixstreams.sinks import BaseSink
from quixstreams.state.base import State
from quixstreams.utils.printing import (
    DEFAULT_COLUMN_NAME,
    DEFAULT_LIVE,
    DEFAULT_LIVE_SLOWDOWN,
)

from .exceptions import InvalidOperation, TopicPartitionsMismatch
from .registry import DataFrameRegistry
from .series import StreamingSeries
from .utils import ensure_milliseconds
from .windows import (
    HoppingCountWindowDefinition,
    HoppingTimeWindowDefinition,
    SlidingCountWindowDefinition,
    SlidingTimeWindowDefinition,
    TumblingCountWindowDefinition,
    TumblingTimeWindowDefinition,
)
from .windows.base import WindowOnLateCallback

if typing.TYPE_CHECKING:
    from quixstreams.processing import ProcessingContext

T = TypeVar("T")

ApplyCallbackStateful = Callable[[Any, State], Any]
ApplyWithMetadataCallbackStateful = Callable[[Any, Any, int, Any, State], Any]
UpdateCallbackStateful = Callable[[Any, State], None]
UpdateWithMetadataCallbackStateful = Callable[[Any, Any, int, Any, State], None]
FilterCallbackStateful = Callable[[Any, State], bool]
FilterWithMetadataCallbackStateful = Callable[[Any, Any, int, Any, State], bool]


class StreamingDataFrame:
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
        *topics: Topic,
        topic_manager: TopicManager,
        registry: DataFrameRegistry,
        processing_context: ProcessingContext,
        stream: Optional[Stream] = None,
        stream_id: Optional[str] = None,
    ):
        if not topics:
            raise ValueError("At least one Topic must be passed")

        # Implicitly deduplicate Topic objects into a tuple and sort them by name
        self._topics: tuple[Topic, ...] = tuple(
            sorted({t.name: t for t in topics}.values(), key=attrgetter("name"))
        )

        self._stream: Stream = stream or Stream()
        self._stream_id: str = stream_id or topic_manager.stream_id_from_topics(
            self.topics
        )
        self._topic_manager = topic_manager
        self._registry = registry
        self._processing_context = processing_context
        self._producer = processing_context.producer
        self._registry.register_stream_id(
            stream_id=self.stream_id, topic_names=[t.name for t in self._topics]
        )

    @property
    def processing_context(self) -> ProcessingContext:
        return self._processing_context

    @property
    def stream(self) -> Stream:
        return self._stream

    @property
    def stream_id(self) -> str:
        """
        An identifier of the data stream this StreamingDataFrame
        manipulates in the application.

        It is used as a common prefix for state stores and group-by topics.
        A new `stream_id` is set when StreamingDataFrames are merged via `.merge()`
        or grouped via `.group_by()`.

        StreamingDataFrames with different `stream_id` cannot access the same state stores.

        By default, a topic name or a combination of topic names are used as `stream_id`.
        """
        return self._stream_id

    @property
    def topics(self) -> tuple[Topic, ...]:
        return self._topics

    @overload
    def apply(
        self,
        func: Union[ApplyCallback, ApplyExpandedCallback],
        *,
        stateful: Literal[False] = False,
        expand: Union[Literal[False], Literal[True]] = False,
        metadata: Literal[False] = False,
    ) -> "StreamingDataFrame": ...

    @overload
    def apply(
        self,
        func: ApplyCallbackStateful,
        *,
        stateful: Literal[True],
        expand: Union[Literal[False], Literal[True]] = False,
        metadata: Literal[False] = False,
    ) -> "StreamingDataFrame": ...

    @overload
    def apply(
        self,
        func: Union[ApplyWithMetadataCallback, ApplyWithMetadataExpandedCallback],
        *,
        stateful: Literal[False] = False,
        expand: Union[Literal[False], Literal[True]] = False,
        metadata: Literal[True],
    ) -> "StreamingDataFrame": ...

    @overload
    def apply(
        self,
        func: ApplyWithMetadataCallbackStateful,
        *,
        stateful: Literal[True],
        expand: Union[Literal[False], Literal[True]] = False,
        metadata: Literal[True],
    ) -> "StreamingDataFrame": ...

    def apply(
        self,
        func: Union[
            ApplyCallback,
            ApplyExpandedCallback,
            ApplyCallbackStateful,
            ApplyWithMetadataCallback,
            ApplyWithMetadataExpandedCallback,
            ApplyWithMetadataCallbackStateful,
        ],
        *,
        stateful: bool = False,
        expand: bool = False,
        metadata: bool = False,
    ) -> "StreamingDataFrame":
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
            if metadata:
                with_metadata_func = cast(ApplyWithMetadataCallbackStateful, func)
            else:
                with_metadata_func = _as_metadata_func(
                    cast(ApplyCallbackStateful, func)
                )

            stateful_func = _as_stateful(
                func=with_metadata_func,
                processing_context=self._processing_context,
                stream_id=self.stream_id,
            )
            stream = self.stream.add_apply(stateful_func, expand=expand, metadata=True)  # type: ignore[call-overload]
        else:
            stream = self.stream.add_apply(
                cast(Union[ApplyCallback, ApplyWithMetadataCallback], func),
                expand=expand,
                metadata=metadata,
            )  # type: ignore[call-overload]
        return self.__dataframe_clone__(stream=stream)

    @overload
    def update(
        self,
        func: UpdateCallback,
        *,
        stateful: Literal[False] = False,
        metadata: Literal[False] = False,
    ) -> "StreamingDataFrame": ...

    @overload
    def update(
        self,
        func: UpdateCallbackStateful,
        *,
        stateful: Literal[True],
        metadata: Literal[False] = False,
    ) -> "StreamingDataFrame": ...

    @overload
    def update(
        self,
        func: UpdateWithMetadataCallback,
        *,
        stateful: Literal[False] = False,
        metadata: Literal[True],
    ) -> "StreamingDataFrame": ...

    @overload
    def update(
        self,
        func: UpdateWithMetadataCallbackStateful,
        *,
        stateful: Literal[True],
        metadata: Literal[True],
    ) -> "StreamingDataFrame": ...

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
    ) -> "StreamingDataFrame":
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
            if metadata:
                with_metadata_func = cast(UpdateWithMetadataCallbackStateful, func)
            else:
                with_metadata_func = _as_metadata_func(
                    cast(UpdateCallbackStateful, func)
                )

            stateful_func = _as_stateful(
                func=with_metadata_func,
                processing_context=self._processing_context,
                stream_id=self.stream_id,
            )
            return self._add_update(stateful_func, metadata=True)
        else:
            return self._add_update(
                cast(Union[UpdateCallback, UpdateWithMetadataCallback], func),
                metadata=metadata,
            )

    @overload
    def filter(
        self,
        func: FilterCallback,
        *,
        stateful: Literal[False] = False,
        metadata: Literal[False] = False,
    ) -> "StreamingDataFrame": ...

    @overload
    def filter(
        self,
        func: FilterCallbackStateful,
        *,
        stateful: Literal[True],
        metadata: Literal[False] = False,
    ) -> "StreamingDataFrame": ...

    @overload
    def filter(
        self,
        func: FilterWithMetadataCallback,
        *,
        stateful: Literal[False] = False,
        metadata: Literal[True],
    ) -> "StreamingDataFrame": ...

    @overload
    def filter(
        self,
        func: FilterWithMetadataCallbackStateful,
        *,
        stateful: Literal[True],
        metadata: Literal[True],
    ) -> "StreamingDataFrame": ...

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
    ) -> "StreamingDataFrame":
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
            if metadata:
                with_metadata_func = cast(FilterWithMetadataCallbackStateful, func)
            else:
                with_metadata_func = _as_metadata_func(
                    cast(FilterCallbackStateful, func)
                )

            stateful_func = _as_stateful(
                func=with_metadata_func,
                processing_context=self._processing_context,
                stream_id=self.stream_id,
            )
            stream = self.stream.add_filter(stateful_func, metadata=True)
        else:
            stream = self.stream.add_filter(  # type: ignore[call-overload]
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
    ) -> "StreamingDataFrame": ...

    @overload
    def group_by(
        self,
        key: Callable[[Any], Any],
        name: str,
        value_deserializer: Optional[DeserializerType] = ...,
        key_deserializer: Optional[DeserializerType] = ...,
        value_serializer: Optional[SerializerType] = ...,
        key_serializer: Optional[SerializerType] = ...,
    ) -> "StreamingDataFrame": ...

    def group_by(
        self,
        key: Union[str, Callable[[Any], Any]],
        name: Optional[str] = None,
        value_deserializer: Optional[DeserializerType] = "json",
        key_deserializer: Optional[DeserializerType] = "json",
        value_serializer: Optional[SerializerType] = "json",
        key_serializer: Optional[SerializerType] = "json",
    ) -> "StreamingDataFrame":
        """
        "Groups" messages by re-keying them via the provided group_by operation
        on their message values.

        This enables things like aggregations on messages with non-matching keys.

        You can provide a column name (uses the column's value) or a custom function
        to generate this new key.

        `.groupby()` can only be performed once per `StreamingDataFrame` instance.

        >**NOTE:** group_by generates a new topic with the `"repartition__"` prefix
            that copies the settings of original topics.

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

        operation = name
        if not operation and isinstance(key, str):
            operation = key

        if not operation:
            raise ValueError(
                'group_by requires "name" parameter when "key" is a function'
            )

        # Generate a config for the new repartition topic based on the underlying topics
        repartition_config = self._topic_manager.derive_topic_config(self._topics)

        # If the topic has only one partition, we don't need a repartition topic
        # we can directly change the messages key as they all go to the same partition.
        if repartition_config.num_partitions == 1:
            return self._single_partition_groupby(operation, key)

        groupby_topic = self._topic_manager.repartition_topic(
            operation=operation,
            stream_id=self.stream_id,
            config=repartition_config,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
            key_deserializer=key_deserializer,
            value_deserializer=value_deserializer,
        )

        self.to_topic(topic=groupby_topic, key=self._groupby_key(key))
        groupby_sdf = self.__dataframe_clone__(groupby_topic)
        self._registry.register_groupby(source_sdf=self, new_sdf=groupby_sdf)
        return groupby_sdf

    def _single_partition_groupby(
        self, operation: str, key: Union[str, Callable[[Any], Any]]
    ) -> "StreamingDataFrame":
        if isinstance(key, str):

            def _callback(value, _, timestamp, headers):
                return value, value[key], timestamp, headers
        else:

            def _callback(value, _, timestamp, headers):
                return value, key(value), timestamp, headers

        stream = self.stream.add_transform(_callback, expand=False)

        groupby_sdf = self.__dataframe_clone__(
            stream=stream, stream_id=f"{self.stream_id}--groupby--{operation}"
        )
        self._registry.register_groupby(
            source_sdf=self, new_sdf=groupby_sdf, register_new_root=False
        )

        return groupby_sdf

    def contains(self, keys: Union[str, list[str]]) -> StreamingSeries:
        """
        Check if keys are present in the Row value.


        Example Snippet:

        ```python
        # Add new column 'has_column' which contains a boolean indicating
        # the presence of 'column_x' and `column_y`

        sdf = StreamingDataFrame()
        sdf['has_column_A'] = sdf.contains('column_a')
        sdf['has_column_X_Y'] = sdf.contains(['column_x', 'column_y'])
        ```


        :param key: column names to check.
        :return: a Column object that evaluates to True if the keys are present
            or False otherwise.
        """
        if isinstance(keys, str):
            _keys = [keys]
        else:
            _keys = keys

        def callback(value, *_):
            return all(k in value for k in _keys)

        return StreamingSeries.from_apply_callback(callback, sdf_id=id(self))

    def to_topic(
        self, topic: Topic, key: Optional[Callable[[Any], Any]] = None
    ) -> "StreamingDataFrame":
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

    def set_timestamp(
        self, func: Callable[[Any, Any, int, Any], int]
    ) -> "StreamingDataFrame":
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

        stream = self.stream.add_transform(_set_timestamp_callback, expand=False)
        return self.__dataframe_clone__(stream=stream)

    def set_headers(
        self,
        func: Callable[
            [Any, Any, int, HeadersTuples],
            HeadersTuples,
        ],
    ) -> "StreamingDataFrame":
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
            headers: HeadersTuples,
        ) -> Tuple[Any, Any, int, HeadersTuples]:
            # Create a shallow copy of original headers to prevent potential mutations
            # of the same collection
            headers = list(headers) if headers else []
            new_headers = func(value, key, timestamp, headers)
            return value, key, timestamp, new_headers

        stream = self.stream.add_transform(func=_set_headers_callback, expand=False)
        return self.__dataframe_clone__(stream=stream)

    def print(
        self, pretty: bool = True, metadata: bool = False
    ) -> "StreamingDataFrame":
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
            printer: Callable[[Any], None] = functools.partial(
                pprint.pprint, indent=2, sort_dicts=False
            )
        else:
            printer = print
        return self._add_update(
            lambda *args: printer({print_args[i]: args[i] for i in range(len(args))}),
            metadata=metadata,
        )

    def print_table(
        self,
        size: int = 5,
        title: Optional[str] = None,
        metadata: bool = True,
        timeout: float = 5.0,
        live: bool = DEFAULT_LIVE,
        live_slowdown: float = DEFAULT_LIVE_SLOWDOWN,
        columns: Optional[List[str]] = None,
        column_widths: Optional[dict[str, int]] = None,
    ) -> "StreamingDataFrame":
        """
        Print a table with the most recent records.

        This feature is experimental and subject to change in future releases.

        Creates a live table view that updates in real-time as new records are processed,
        showing the most recent N records in a formatted table. When metadata is enabled,
        the table includes message metadata columns (_key, _timestamp) along with the
        record values.

        The table automatically adjusts to show all available columns unless specific
        columns are requested. Missing values in any column are displayed as empty cells.
        Column widths adjust automatically to fit content unless explicitly specified.

        Note: Column overflow is not handled gracefully. If your data has many columns,
        the table may become unreadable. Use the `columns` parameter to specify which
        columns to display and/or `column_widths` to control column sizes for better
        visibility.

        Printing Behavior:
        - Interactive mode (terminal/console): The table refreshes in-place, with new
          rows appearing at the bottom and old rows being removed from the top when
          the table is full.
        - Non-interactive mode (output redirected to file): Collects records until
          either the table is full or the timeout is reached, then prints the complete
          table and starts collecting new records.

        Note: This works best in terminal environments. For Jupyter notebooks,
        consider using `print()` instead.

        Note: The last provided live value will be used for all print_table calls
        in the pipeline.

        Note: The last provided live_slowdown value will be used for all print_table calls
        in the pipeline.

        Example Snippet:

        ```python
        sdf = app.dataframe(topic)
        # Show last 5 records, update at most every 1 second
        sdf.print_table(size=5, title="Live Records", slowdown=1)
        ```

        This will produce a live-updating table like this:

        Live Records
        ┏━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┳━━━━━┳━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━┓
        ┃ _key       ┃ _timestamp ┃ active ┃ id  ┃ name    ┃ score ┃ status   ┃
        ┡━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━╇━━━━━╇━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━┩
        │ b'53fe8e4' │ 1738685136 │ True   │ 876 │ Charlie │ 27.74 │ pending  │
        │ b'91bde51' │ 1738685137 │ True   │ 11  │         │       │ approved │
        │ b'6617dfe' │ 1738685138 │        │     │ David   │       │          │
        │ b'f47ac93' │ 1738685139 │        │ 133 │         │       │          │
        │ b'038e524' │ 1738685140 │ False  │     │         │       │          │
        └────────────┴────────────┴────────┴─────┴─────────┴───────┴──────────┘

        :param size: Maximum number of records to display in the table. Default: 5
        :param title: Optional title for the table
        :param metadata: Whether to include message metadata (_key, _timestamp) columns.
            Default: True
        :param timeout: Time in seconds to wait for table to fill up before printing
            an incomplete table. Only relevant for non-interactive environments
            (e.g. output redirected to a file). Default: 5.0
        :param live: Whether to print the table in real-time if possible.
            If real-time printing is not possible, the table will be printed
            in non-interactive mode. Default: True
        :param live_slowdown: Time in seconds to wait between live table updates.
            Increase this value if the table updates too quickly.
            Default: 0.5 seconds.
        :param columns: Optional list of columns to display. If not provided,
            all columns will be displayed. Pass empty list to display only metadata.
        :param column_widths: Optional dictionary mapping column names to their desired
            widths in characters. If not provided, column widths will be determined
            automatically based on content. Example: {"name": 20, "id": 10}
        """
        printer = self.processing_context.printer
        printer.configure_live(live, live_slowdown)

        if columns is not None:
            if metadata:
                columns = ["_key", "_timestamp", *columns]
            elif len(columns) == 0:
                warnings.warn(
                    "`columns` is an empty list and `metadata` is False. "
                    f"Table `{title}` will be empty."
                )

        table = printer.add_table(
            size=size,
            title=title,
            timeout=timeout,
            columns=columns,
            column_widths=column_widths,
        )

        def _add_row(value: Any, *_metadata: tuple[Any, int, HeadersTuples]) -> None:
            if not isinstance(value, dict):
                value = {DEFAULT_COLUMN_NAME: value}
            if metadata:
                value = dict(_key=_metadata[0], _timestamp=_metadata[1], **value)
            table.add_row(value)

        return self._add_update(_add_row, metadata=metadata)

    def compose(
        self,
        sink: Optional[VoidExecutor] = None,
    ) -> dict[str, VoidExecutor]:
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
        key: Any = b"key",
        timestamp: int = 0,
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
            if len(self.topics) > 1:
                raise ValueError("Topic must be provided for the multi-topic SDF")
            topic = self.topics[0]
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
        on_late: Optional[WindowOnLateCallback] = None,
    ) -> TumblingTimeWindowDefinition:
        """
        Create a time-based tumbling window transformation on this StreamingDataFrame.
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
        from quixstreams import Application
        import quixstreams.dataframe.windows.aggregations as agg

        app = Application()
        sdf = app.dataframe(...)

        sdf = (
            # Define a tumbling window of 60s and grace period of 10s
            sdf.tumbling_window(
                duration_ms=timedelta(seconds=60), grace_ms=timedelta(seconds=10.0)
            )

            # Specify the aggregation function
            .agg(value=agg.Sum())

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

        :param on_late: an optional callback to react on late records in windows and
            to configure the logging of such events.
            If the callback returns `True`, the message about a late record will be logged
            (default behavior).
            Otherwise, no message will be logged.

        :return: `TumblingTimeWindowDefinition` instance representing the tumbling window
            configuration.
            This object can be further configured with aggregation functions
            like `sum`, `count`, etc. applied to the StreamingDataFrame.

        """
        duration_ms = ensure_milliseconds(duration_ms)
        grace_ms = ensure_milliseconds(grace_ms)

        return TumblingTimeWindowDefinition(
            duration_ms=duration_ms,
            grace_ms=grace_ms,
            dataframe=self,
            name=name,
            on_late=on_late,
        )

    def tumbling_count_window(
        self, count: int, name: Optional[str] = None
    ) -> TumblingCountWindowDefinition:
        """
        Create a count-based tumbling window transformation on this StreamingDataFrame.
        Tumbling windows divide messages into fixed-batch, non-overlapping windows.
        They allow performing stateful aggregations like `sum`, `reduce`, etc.
        on top of the data and emit results downstream.
        Notes:
        - The start timestamp of the aggregation result is set to the earliest timestamp.
        - The end timestamp of the aggregation result is set to the latest timestamp.
        - Every window is grouped by the current Kafka message key.
        - Messages with `None` key will be ignored.


        Example Snippet:

        ```python
        from quixstreams import Application
        import quixstreams.dataframe.windows.aggregations as agg

        app = Application()
        sdf = app.dataframe(...)
        sdf = (
            # Define a tumbling window of 10 messages
            sdf.tumbling_count_window(count=10)
            # Specify the aggregation function
            .agg(value=agg.Sum())
            # Specify how the results should be emitted downstream.
            # "current()" will emit results as they come for each updated window,
            # possibly producing multiple messages per key-window pair
            # "final()" will emit windows only when they are closed and cannot
            # receive any updates anymore.
            .current()
        )
        ```
        :param count: The length of each window. The number of messages to include in the window.
        :param name: The unique identifier for the window. If not provided, it will be
            automatically generated based on the window's properties.
        :return: `TumblingCountWindowDefinition` instance representing the tumbling window
            configuration.
            This object can be further configured with aggregation functions
            like `sum`, `count`, etc. applied to the StreamingDataFrame.
        """
        return TumblingCountWindowDefinition(
            count=count,
            dataframe=self,
            name=name,
        )

    def hopping_window(
        self,
        duration_ms: Union[int, timedelta],
        step_ms: Union[int, timedelta],
        grace_ms: Union[int, timedelta] = 0,
        name: Optional[str] = None,
        on_late: Optional[WindowOnLateCallback] = None,
    ) -> HoppingTimeWindowDefinition:
        """
        Create a time-based hopping window transformation on this StreamingDataFrame.
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
        from quixstreams import Application
        import quixstreams.dataframe.windows.aggregations as agg

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
            .agg(value=agg.Sum())

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

        :param on_late: an optional callback to react on late records in windows and
            to configure the logging of such events.
            If the callback returns `True`, the message about a late record will be logged
            (default behavior).
            Otherwise, no message will be logged.

        :return: `HoppingTimeWindowDefinition` instance representing the hopping
            window configuration.
            This object can be further configured with aggregation functions
            like `sum`, `count`, etc. and applied to the StreamingDataFrame.
        """

        duration_ms = ensure_milliseconds(duration_ms)
        step_ms = ensure_milliseconds(step_ms)
        grace_ms = ensure_milliseconds(grace_ms)

        return HoppingTimeWindowDefinition(
            duration_ms=duration_ms,
            grace_ms=grace_ms,
            step_ms=step_ms,
            dataframe=self,
            name=name,
            on_late=on_late,
        )

    def hopping_count_window(
        self, count: int, step: int, name: Optional[str] = None
    ) -> HoppingCountWindowDefinition:
        """
        Create a count-based hopping window transformation on this StreamingDataFrame.
        Hopping windows divide the data stream into overlapping windows.
        The overlap is controlled by the `step` parameter.
        They allow performing stateful aggregations like `sum`, `reduce`, etc.
        on top of the data and emit results downstream.
        Notes:
        - The start timestamp of the aggregation result is set to the earliest timestamp.
        - The end timestamp of the aggregation result is set to the latest timestamp.
        - Every window is grouped by the current Kafka message key.
        - Messages with `None` key will be ignored.


        Example Snippet:

        ```python
        from quixstreams import Application
        import quixstreams.dataframe.windows.aggregations as agg

        app = Application()
        sdf = app.dataframe(...)
        sdf = (
            # Define a hopping window of 10 messages with a step of 5 messages
            sdf.hopping_count_window(
                count=10,
                step=5,
            )
            # Specify the aggregation function
            .agg(value=agg.Sum())
            # Specify how the results should be emitted downstream.
            # "current()" will emit results as they come for each updated window,
            # possibly producing multiple messages per key-window pair
            # "final()" will emit windows only when they are closed and cannot
            # receive any updates anymore.
            .current()
        )
        ```
        :param count: The length of each window. The number of messages to include in the window.
        :param step: The step size for the window. It determines the number of messages between windows.
            A  sliding windows is the same as a hopping window with a step of 1 message.
        :param name: The unique identifier for the window. If not provided, it will be
            automatically generated based on the window's properties.
        :return: `HoppingCountWindowDefinition` instance representing the hopping
            window configuration.
            This object can be further configured with aggregation functions
            like `sum`, `count`, etc. and applied to the StreamingDataFrame.
        """
        return HoppingCountWindowDefinition(
            count=count,
            dataframe=self,
            step=step,
            name=name,
        )

    def sliding_window(
        self,
        duration_ms: Union[int, timedelta],
        grace_ms: Union[int, timedelta] = 0,
        name: Optional[str] = None,
        on_late: Optional[WindowOnLateCallback] = None,
    ) -> SlidingTimeWindowDefinition:
        """
        Create a time-based sliding window transformation on this StreamingDataFrame.
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
        from quixstreams import Application
        import quixstreams.dataframe.windows.aggregations as agg

        app = Application()
        sdf = app.dataframe(...)

        sdf = (
            # Define a sliding window of 60s with a grace period of 10s
            sdf.sliding_window(
                duration_ms=timedelta(seconds=60),
                grace_ms=timedelta(seconds=10)
            )

            # Specify the aggregation function
            .agg(value=agg.Sum())

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

        :param on_late: an optional callback to react on late records in windows and
            to configure the logging of such events.
            If the callback returns `True`, the message about a late record will be logged
            (default behavior).
            Otherwise, no message will be logged.

        :return: `SlidingTimeWindowDefinition` instance representing the sliding window
            configuration.
            This object can be further configured with aggregation functions
            like `sum`, `count`, etc. applied to the StreamingDataFrame.
        """

        duration_ms = ensure_milliseconds(duration_ms)
        grace_ms = ensure_milliseconds(grace_ms)

        return SlidingTimeWindowDefinition(
            duration_ms=duration_ms,
            grace_ms=grace_ms,
            dataframe=self,
            name=name,
            on_late=on_late,
        )

    def sliding_count_window(
        self, count: int, name: Optional[str] = None
    ) -> SlidingCountWindowDefinition:
        """
        Create a count-based sliding window transformation on this StreamingDataFrame.
        Sliding windows continuously evaluate the stream with a fixed step of 1 message
        allowing for overlapping, but not redundant windows of a fixed size.
        Sliding windows are similar to hopping windows with step set to 1.
        They allow performing stateful aggregations like `sum`, `reduce`, etc.
        on top of the data and emit results downstream.
        Notes:
        - The start timestamp of the aggregation result is set to the earliest timestamp.
        - The end timestamp of the aggregation result is set to the latest timestamp.
        - Every window is grouped by the current Kafka message key.
        - Messages with `None` key will be ignored.
        - Every window contains a distinct aggregation.


        Example Snippet:

        ```python
        from quixstreams import Application
        import quixstreams.dataframe.windows.aggregations as agg

        app = Application()
        sdf = app.dataframe(...)
        sdf = (
            # Define a sliding window of 10 messages
            sdf.sliding_count_window(count=10)
            # Specify the aggregation function
            .sum(value=agg.Sum())
            # Specify how the results should be emitted downstream.
            # "current()" will emit results as they come for each updated window,
            # possibly producing multiple messages per key-window pair
            # "final()" will emit windows only when they are closed and cannot
            # receive any updates anymore.
            .current()
        )
        ```
        :param count: The length of each window. The number of messages to include in the window.
        :param name: The unique identifier for the window. If not provided, it will be
            automatically generated based on the window's properties.
        :return: `SlidingCountWindowDefinition` instance representing the sliding window
            configuration.
            This object can be further configured with aggregation functions
            like `sum`, `count`, etc. applied to the StreamingDataFrame.
        """
        return SlidingCountWindowDefinition(
            count=count,
            dataframe=self,
            name=name,
        )

    def fill(self, *columns: str, **mapping: Any) -> "StreamingDataFrame":
        """
        Fill missing values in the message value with a constant value.

        This operation occurs in-place, meaning reassignment is entirely OPTIONAL: the
        original `StreamingDataFrame` is returned for chaining (`sdf.update().print()`).

        Example Snippets:

        Fill missing values for a single column with a None:
        ```python
        # This would transform {"x": 1} to {"x": 1, "y": None}
        sdf.fill("y")
        ```

        Fill missing values for multiple columns with a None:
        ```python
        # This would transform {"x": 1} to {"x": 1, "y": None, "z": None}
        sdf.fill("y", "z")
        ```

        Fill missing values in the value with a constant value using a dictionary:
        ```python
        # This would transform {"x": None} to {"x": 1, "y": 2}
        sdf.fill(x=1, y=2)
        ```

        Use a combination of positional and keyword arguments:
        ```python
        # This would transform {"y": None} to {"x": None, "y": 2}
        sdf.fill("x", y=2)
        ```

        :param columns: a list of column names as strings.
        :param mapping: a dictionary where keys are column names and values are the fill values.
        :return: the original `StreamingDataFrame` instance for chaining.
        """
        mapping = {**{column: None for column in columns}, **mapping}

        if not mapping:
            raise ValueError("No columns or mapping provided to fill().")

        def _fill(value: Any) -> Any:
            if not isinstance(value, dict):
                return value
            for column, fill_value in mapping.items():
                if value.get(column) is None:
                    value[column] = fill_value
            return value

        return self._add_update(_fill, metadata=False)

    def drop(
        self,
        columns: Union[str, List[str]],
        errors: Literal["ignore", "raise"] = "raise",
    ) -> "StreamingDataFrame":
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
            value: Any, key: Any, timestamp: int, headers: HeadersTuples
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

    def concat(self, other: "StreamingDataFrame") -> "StreamingDataFrame":
        """
        Concatenate two StreamingDataFrames together and return a new one.
        The transformations applied on this new StreamingDataFrame will update data
        from both origins.

        Use it to concatenate dataframes belonging to different topics as well as to merge the branches
        of the same original dataframe.

        If concatenated dataframes belong to different topics, the stateful operations
        on the new dataframe will create different state stores
        unrelated to the original dataframes and topics.
        The same is true for the repartition topics created by `.group_by()`.

        :param other: other StreamingDataFrame
        :return: a new StreamingDataFrame
        """

        merged_stream = self.stream.merge(other.stream)

        # Enable partition time alignment only when concatenated StreamingDataFrames
        # have different topics (they could be just branches)
        total_topics = {t.name for t in itertools.chain(self.topics, other.topics)}
        if len(total_topics) > 1:
            self._registry.require_time_alignment()

        return self.__dataframe_clone__(
            *self.topics, *other.topics, stream=merged_stream
        )

    def ensure_topics_copartitioned(self):
        partitions_counts = set(t.broker_config.num_partitions for t in self._topics)
        if len(partitions_counts) > 1:
            msg = ", ".join(
                f'"{t.name}" ({t.broker_config.num_partitions} partitions)'
                for t in self._topics
            )
            raise TopicPartitionsMismatch(
                f"The underlying topics must have the same number of partitions to use State; got {msg}"
            )

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
        self._stream = self._stream.add_update(func, metadata=metadata)  # type: ignore[call-overload]
        return self

    def _register_store(self):
        """
        Register the default store for the current stream_id in StateStoreManager.
        """
        self.ensure_topics_copartitioned()

        # Generate a changelog topic config based on the underlying topics.
        changelog_topic_config = self._topic_manager.derive_topic_config(self._topics)

        self._processing_context.state_manager.register_store(
            stream_id=self.stream_id, changelog_config=changelog_topic_config
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
        *topics: Topic,
        stream: Optional[Stream] = None,
        stream_id: Optional[str] = None,
    ) -> "StreamingDataFrame":
        """
        Clone the StreamingDataFrame with a new `stream`, `topics`,
        and optional `stream_id` parameters.

        :param topics: one or more `Topic` objects
        :param stream: instance of `Stream`, optional.
        :return: a new `StreamingDataFrame`.
        """

        clone = self.__class__(
            *(topics or self._topics),
            stream=stream,
            stream_id=stream_id,
            processing_context=self._processing_context,
            topic_manager=self._topic_manager,
            registry=self._registry,
        )
        return clone

    def __setitem__(self, item_key: Any, item: Union["StreamingDataFrame", object]):
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
    def __getitem__(
        self, item: Union[StreamingSeries, List[str], "StreamingDataFrame"]
    ) -> "StreamingDataFrame": ...

    def __getitem__(
        self, item: Union[str, List[str], StreamingSeries, "StreamingDataFrame"]
    ) -> Union["StreamingDataFrame", StreamingSeries]:
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
    func: Callable[[Any, State], T],
) -> Callable[[Any, Any, int, Any, State], T]:
    @functools.wraps(func)
    def wrapper(
        value: Any, _key: Any, _timestamp: int, _headers: Any, state: State
    ) -> Any:
        return func(value, state)

    return wrapper


def _as_stateful(
    func: Callable[[Any, Any, int, Any, State], T],
    processing_context: ProcessingContext,
    stream_id: str,
) -> Callable[[Any, Any, int, Any], T]:
    @functools.wraps(func)
    def wrapper(value: Any, key: Any, timestamp: int, headers: Any) -> Any:
        ctx = message_context()
        transaction = processing_context.checkpoint.get_store_transaction(
            stream_id=stream_id,
            partition=ctx.partition,
        )
        # Pass a State object with an interface limited to the key updates only
        # and prefix all the state keys by the message key
        state = transaction.as_state(prefix=key)
        return func(value, key, timestamp, headers, state)

    return wrapper
