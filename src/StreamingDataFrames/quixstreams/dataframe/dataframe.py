import uuid
from typing import Optional, Callable, Union, List, Mapping, Any

from typing_extensions import Self, TypeAlias

from .column import Column
from .exceptions import InvalidApplyResultType
from .pipeline import Pipeline
from ..models import Row, Topic, MessageContext
from ..rowconsumer import RowConsumerProto
from ..rowproducer import RowProducerProto
from ..state import State, StateStoreManager

ApplyFunc: TypeAlias = Callable[
    [dict, MessageContext], Optional[Union[dict, List[dict]]]
]
StatefulApplyFunc: TypeAlias = Callable[
    [dict, MessageContext, State], Optional[Union[dict, List[dict]]]
]
KeyFunc: TypeAlias = Callable[[dict, MessageContext], Any]

__all__ = ("StreamingDataFrame",)


def subset(keys: List[str], row: Row) -> Row:
    row.value = row[keys]
    return row


def setitem(k: str, v: Any, row: Row) -> Row:
    row[k] = v.eval(row) if isinstance(v, Column) else v
    return row


def apply(
    row: Row,
    func: Union[ApplyFunc, StatefulApplyFunc],
    state_manager: Optional[StateStoreManager] = None,
) -> Union[Row, List[Row]]:
    # Providing state to the function if state_manager is passed
    if state_manager is not None:
        transaction = state_manager.get_store_transaction()
        # Prefix all the state keys by the message key
        with transaction.with_prefix(prefix=row.key):
            # Pass a State object with an interface limited to the key updates only
            result = func(row.value, row.context, transaction.state)
    else:
        result = func(row.value, row.context)

    if result is None and isinstance(row.value, dict):
        # Function returned None, assume it changed the incoming dict in-place
        return row
    if isinstance(result, dict):
        # Function returned dict, assume it's a new value for the Row
        row.value = result
        return row
    raise InvalidApplyResultType(
        f"Only 'dict' or 'NoneType' (in-place modification) allowed, not {type(result)}"
    )


class StreamingDataFrame:
    """
    Allows you to define transformations on a kafka message as if it were a Pandas
    DataFrame.
    Currently, it implements a small subset of the Pandas interface, along with
    some differences/accommodations for kafka-specific functionality.

    A `StreamingDataFrame` expects to interact with a QuixStreams `Row`, which is
    interacted with like a dictionary.

    Unlike pandas, you will not get an immediate output from any given operation;
    instead, the command is permanently added to the `StreamingDataFrame`'s
    "pipeline". You can then execute this pipeline indefinitely on a `Row` like so:

    ```
    df = StreamingDataframe(topic)
    df = df.apply(lambda row: row) # do stuff; must return the row back!
    for row_obj in [row_0, row_1]:
        print(df.process(row_obj))
    ```

    Note that just like Pandas, you can "filter" out rows with your operations, like:
    ```
    df = df[df['column_b'] >= 5]
    ```
    If a processing step nulls the Row in some way, all further processing on that
    row (including kafka operations, besides committing) will be skipped.

    There is a :class:`quixstreams.app.Application` class that can manage
    the kafka-specific dependencies of `StreamingDataFrame`;
    it is recommended you hand your `StreamingDataFrame` instance to an `Application`
    instance when interacting with Kafka.

    Below is a larger example of a `StreamingDataFrame`
    (that you'd hand to an `Application`):

    ```
    # Define your processing steps
    # Remove column_a, add 1 to columns b and c, skip row if b+1 >= 5, else publish row
    df = StreamingDataframe()
    df = df[['column_b', 'column_c']]
    df = df.apply(lambda row: row[key] + 1 if key in ['column_b', 'column_c'])
    df = df[df['column_b'] + 1] >= 5]
    df.to_topic('my_output_topic')

    # Incomplete Rows to showcase what data you are actually interacting with
    record_0 = Row(value={'column_a': 'a_string', 'column_b': 3, 'column_c': 5})
    record_1 = Row(value={'column_a': 'a_string', 'column_b': 1, 'column_c': 10})

    # process records
    df.process(record_0)  # produces {'column_b': 4, 'column_c': 6} to "my_output_topic"
    df.process(record_1)  # filters row, does NOT produce to "my_output_topic"
    ```
    """

    def __init__(
        self,
        topic: Topic,
        state_manager: StateStoreManager,
    ):
        self._id = str(uuid.uuid4())
        self._pipeline = Pipeline(_id=self.id)
        self._real_consumer: Optional[RowConsumerProto] = None
        self._real_producer: Optional[RowProducerProto] = None
        self._topics_in = {topic.name: topic}
        self._topics_out = {}
        self._state_manager = state_manager

    def apply(
        self,
        func: Union[ApplyFunc, StatefulApplyFunc],
        stateful: bool = False,
    ) -> Self:
        """
        Apply a custom function with current value
        and :py:class:`quixstreams.models.context.MessageContext`
        as the expected input.

        :param func: a callable which accepts 2 arguments:
            - value - a dict with fields and values for the current Row
            - a context - an instance of :py:class:`quixstreams.models.context.MessageContext`
                which contains message metadata like key, timestamp, partition,
                and more.

            .. note:: if `stateful=True` is passed, a third argument of type `State`
                will be provided to the function.

            The custom function may return:
            - a new dict to replace the current Row value
            - `None` to modify the current Row value in-place

        :param stateful: if `True`, the function will be provided with 3rd argument
            of type `State` to perform stateful operations.

        :return: current instance of `StreamingDataFrame`
        """

        if stateful:
            # Register the default store for each input topic
            for topic in self._topics_in.values():
                self._state_manager.register_store(topic_name=topic.name)
            return self._apply(
                lambda row: apply(row, func, state_manager=self._state_manager)
            )
        return self._apply(lambda row: apply(row, func))

    def process(self, row: Row) -> Optional[Union[Row, List[Row]]]:
        """
        Execute the previously defined StreamingDataframe operations on a provided Row.
        :param row: a QuixStreams Row object
        :return: Row, list of Rows, or None (if filtered)
        """
        return self._pipeline.process(row)

    def to_topic(self, topic: Topic, key: Optional[KeyFunc] = None) -> Self:
        """
        Produce a row to a desired topic.
        Note that a producer must be assigned on the `StreamingDataFrame` if not using
        :class:`quixstreams.app.Application` class to facilitate
        the execution of StreamingDataFrame.

        :param topic: A QuixStreams `Topic`
        :param key: a callable to generate a new message key, optional.
            If passed, the return type of this callable must be serializable
            by `key_serializer` defined for this Topic object.
            By default, the current message key will be used.

        :return: self (StreamingDataFrame)
        """
        self._topics_out[topic.name] = topic
        return self._apply(
            lambda row: self._produce(
                topic, row, key=key(row.value, row.context) if key else None
            )
        )

    @property
    def id(self) -> str:
        return self._id

    @property
    def topics_in(self) -> Mapping[str, Topic]:
        """
        Get a mapping with Topics for the StreamingDataFrame input topics
        :return: dict of {<topic_name>: <Topic>}
        """
        return self._topics_in

    @property
    def topics_out(self) -> Mapping[str, Topic]:
        """
        Get a mapping with Topics for the StreamingDataFrame output topics
        :return: dict of {<topic_name>: <Topic>}
        """
        return self._topics_out

    @property
    def consumer(self) -> RowConsumerProto:
        if self._real_consumer is None:
            raise RuntimeError("Consumer instance has not been provided")
        return self._real_consumer

    @consumer.setter
    def consumer(self, consumer: RowConsumerProto):
        self._real_consumer = consumer

    @property
    def producer(self) -> RowProducerProto:
        if self._real_producer is None:
            raise RuntimeError("Producer instance has not been provided")
        return self._real_producer

    @producer.setter
    def producer(self, producer: RowProducerProto):
        self._real_producer = producer

    def __setitem__(self, key: str, value: Any):
        self._apply(lambda row: setitem(key, value, row))

    def __getitem__(
        self, item: Union[str, List[str], Column, Self]
    ) -> Union[Column, Self]:
        if isinstance(item, Column):
            return self._apply(lambda row: row if item.eval(row) else None)
        elif isinstance(item, list):
            return self._apply(lambda row: subset(item, row))
        elif isinstance(item, StreamingDataFrame):
            # TODO: Implement filtering based on another SDF
            raise ValueError(
                "Filtering based on StreamingDataFrame is not supported yet."
            )
        else:
            return Column(col_name=item)

    def _produce(self, topic: Topic, row: Row, key: Optional[Any] = None) -> Row:
        self.producer.produce_row(row, topic, key=key)
        return row

    def _apply(self, func: Callable[[Row], Optional[Union[Row, List[Row]]]]) -> Self:
        """
        Add a callable to the StreamingDataframe execution list.
        The provided callable should accept and return a Quixstreams Row; exceptions
        to this include a user's .apply() function, or a "filter" that returns None.

        :param func: callable that accepts and (usually) returns a QuixStreams Row
        :return: self (StreamingDataFrame)
        """
        self._pipeline.apply(func)
        return self
