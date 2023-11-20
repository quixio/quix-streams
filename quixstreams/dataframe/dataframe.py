import contextvars
import functools
import operator
from typing import Optional, Callable, Union, List, TypeVar, Any

from typing_extensions import Self

from quixstreams.context import (
    message_context,
    set_message_context,
    message_key,
)
from quixstreams.core.stream import StreamCallable, Stream
from quixstreams.models import Topic, Row, MessageContext
from quixstreams.rowproducer import RowProducerProto
from quixstreams.state import StateStoreManager, State
from .base import BaseStreaming
from .series import StreamingSeries

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

    <blockquote>
    Random methods for example purposes. More detailed explanations found under
    various methods or in the docs folder.
    ```python
    sdf = StreamingDataframe()
    sdf = sdf.apply(a_func)
    sdf["my_new_bool_field"] = sdf["a_column_name"].contains("a_str")
    ```
    </blockquote>


    """

    def __init__(
        self,
        topic: Topic,
        state_manager: StateStoreManager,
        stream: Optional[Stream] = None,
    ):
        self._stream: Stream = stream or Stream()
        self._topic = topic
        self._real_producer: Optional[RowProducerProto] = None
        self._state_manager = state_manager

    @property
    def stream(self) -> Stream:
        return self._stream

    @property
    def topic(self) -> Topic:
        return self._topic

    def apply(
        self, func: Union[DataFrameFunc, DataFrameStatefulFunc], stateful: bool = False
    ) -> Self:
        """
        Apply a function to transform the value and return a new value.

        The result will be passed downstream as an input value.


        Example Snippet:

        <blockquote>
        This stores a string in state and capitalizes every column with a string value.
        <br>
        A second apply then keeps only the string value columns (shows non-stateful).

        ```python
        def func(d: dict, state: State):
            value = d["store_field"]
            if value != state.get("my_store_key"):
                state.set("my_store_key") = value
            return {k: v.upper() if isinstance(v, str) else v for k, v in d.items()}

        sdf = StreamingDataframe()
        sdf = sdf.apply(func, stateful=True)
        sdf = sdf.apply(lambda d: {k: v for k,v in d.items() if isinstance(v, str)})

        ```
        </blockquote>


        :param func: a function to apply
        :param stateful: if `True`, the function will be provided with a second argument
            of type `State` to perform stateful operations.
        """
        if stateful:
            self._register_store()
            func = _as_stateful(func=func, state_manager=self._state_manager)

        stream = self.stream.add_apply(func)
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

        <blockquote>
        Stores a value and mutates a list by appending a new item to it.
        <br>
        Also prints to console.

        ```python
        def func(values: list, state: State):
            value = values[0]
            if value != state.get("my_store_key"):
                state.set("my_store_key") = value
            values.append("new_item")

        sdf = StreamingDataframe()
        sdf = sdf.update(func, stateful=True)
        sdf = sdf.update(lambda value: print("Received value: ", value))
        ```
        </blockquote>

        :param func: function to update value
        :param stateful: if `True`, the function will be provided with a second argument
            of type `State` to perform stateful operations.
        """
        if stateful:
            self._register_store()
            func = _as_stateful(func=func, state_manager=self._state_manager)

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

        <blockquote>
        Stores a value and allows further processing only if the value is greater than
        what was previously stored.

        ```python
        def func(d: dict, state: State):
            value = d["my_value"]
            if value > state.get("my_store_key"):
                state.set("my_store_key") = value
                return True
            return False

        sdf = StreamingDataframe()
        sdf = sdf.filter(func, stateful=True)
        ```
        </blockquote>


        :param func: function to filter value
        :param stateful: if `True`, the function will be provided with second argument
            of type `State` to perform stateful operations.
        """

        if stateful:
            self._register_store()
            func = _as_stateful(func=func, state_manager=self._state_manager)

        stream = self.stream.add_filter(func)
        return self._clone(stream=stream)

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

        <blockquote>
        Add new column 'has_column' which contains a boolean indicating the presence of
        'column_x'.

        ```python
        sdf = StreamingDataframe()
        sdf['has_column'] = sdf.contains('column_x')
        ```
        </blockquote>


        :param key: a column name to check.
        :returns: a Column object that evaluates to True if the key is present or
        False otherwise.
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

        <blockquote>
        Produce to two different topics, changing the key for one of them.
        <br>
        Uses the Application class (sans arguments) to showcase how you'd commonly
        use this.

        ```python
        from quixstreams import Application
        app = Application()
        input_topic = app.topic("input_x")
        output_topic_0 = app.topic("output_a")
        output_topic_1 = app.topic("output_b")

        sdf = app.dataframe(input_topic)
        sdf = sdf.to_topic(output_topic_0)
        sdf = sdf.to_topic(output_topic_1, key=lambda data: data["a_field"])
        ```
        </blockquote>

        :param topic: instance of `Topic`
        :param key: a callable to generate a new message key, optional.
            If passed, the return type of this callable must be serializable
            by `key_serializer` defined for this Topic object.
            By default, the current message key will be used.

        """
        return self.update(
            lambda value: self._produce(topic, value, key=key(value) if key else None)
        )

    def compile(self) -> StreamCallable:
        """
        Compile all functions of this StreamingDataFrame into one big closure.

        Closures are more performant than calling all the functions in the
        `StreamingDataFrame` one-by-one.

        Generally not required by users; the `quixstreams.app.Application` class will
        compile automatically for you (along with calling it with values, of course)!


        Example Snippet:

        <blockquote>
        After all sdf commands have been made we then compile, which can then be called
        with any values we desire to process them.

        ```python
        from quixstreams import Application
        sdf = app.dataframe()
        sdf = sdf.apply(apply_func)
        sdf = sdf.filter(filter_func)
        sdf = sdf.compile()

        result_0 = sdf({"my": "record"})
        result_1 = sdf({"other": "record"})
        </blockquote>


        :return: a function that accepts "value"
            and returns a result of StreamingDataFrame
        """
        return self.stream.compile()

    def test(self, value: object, ctx: Optional[MessageContext] = None) -> Any:
        """
        A shorthand to test `StreamingDataFrame` with provided value
        and `MessageContext`.

        :param value: value to pass through `StreamingDataFrame`
        :param ctx: instance of `MessageContext`, optional.
            Provide it if the StreamingDataFrame instance calls `to_topic()`,
            has stateful functions or functions calling `get_current_key()`.
            Default - `None`.

        :return: result of `StreamingDataFrame`
        """
        context = contextvars.copy_context()
        context.run(set_message_context, ctx)
        compiled = self.compile()
        return context.run(compiled, value)

    def _clone(self, stream: Stream) -> Self:
        clone = self.__class__(
            stream=stream, topic=self._topic, state_manager=self._state_manager
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
        self._state_manager.register_store(topic_name=self._topic.name)

    def __setitem__(self, key, value: Union[Self, object]):
        if isinstance(value, self.__class__):
            diff = self.stream.diff(value.stream)
            diff_compiled = diff.compile(allow_filters=False, allow_updates=False)
            stream = self.stream.add_update(
                lambda v: operator.setitem(v, key, diff_compiled(v))
            )
        elif isinstance(value, StreamingSeries):
            value_compiled = value.compile(allow_filters=False, allow_updates=False)
            stream = self.stream.add_update(
                lambda v: operator.setitem(v, key, value_compiled(v))
            )
        else:
            stream = self.stream.add_update(lambda v: operator.setitem(v, key, value))
        self._stream = stream

    def __getitem__(
        self, item: Union[str, List[str], StreamingSeries, Self]
    ) -> Union[Self, StreamingSeries]:
        if isinstance(item, StreamingSeries):
            # Filter SDF based on StreamingSeries
            item_compiled = item.compile(allow_filters=False, allow_updates=False)
            return self.filter(lambda v: item_compiled(v))
        elif isinstance(item, self.__class__):
            # Filter SDF based on another SDF
            diff = self.stream.diff(item.stream)
            diff_compiled = diff.compile(allow_filters=False, allow_updates=False)
            return self.filter(lambda v: diff_compiled(v))
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
