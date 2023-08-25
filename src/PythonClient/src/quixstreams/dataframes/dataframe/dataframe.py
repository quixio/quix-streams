import uuid
from typing import Self, Optional, Callable, TypeAlias, Union, List, Mapping

from .column import Column, OpValue
from .pipeline import Pipeline
from ..models import Row, Topic
from ..rowconsumer import RowConsumerProto
from ..rowproducer import RowProducerProto

RowApplier: TypeAlias = Callable[[Row], Optional[Union[Row, list[Row]]]]

__all__ = ("StreamingDataFrame",)


def subset(keys: list[str], row: Row) -> Row:
    row.value = row[keys]
    return row


def setitem(k: str, v: Union[Column, OpValue], row: Row) -> Row:
    row[k] = v.eval(row) if isinstance(v, Column) else v
    return row


class StreamingDataFrame:
    """
    Allows you to define transformations on a kafka message as if it were a Pandas
    DataFrame. The interface currently includes a small subset of current Pandas
    functionality, along with having some distinct differences or unique additions
    to accommodate kafka-specific functionality. These distinctions will be made
    clear where possible either in respective method docstrings or otherwise.

    StreamingDataFrame's expect to interact with QuixStreams Rows, which function
    much like dictionaries, but have other kafka-related metadata as well (see `Row`
    class for more detail).

    Note that unlike pandas, you will not get an immediate output from any given
    operation; instead, the command is permanently added to the StreamingDataFrame's
    "pipeline". When ready, you can execute the StreamingDataFrame's pipeline by calling
    `.process()` on the StreamingDataFrame instance with a Row instance.

    For example, with Row `my_row` and StreamingDataFrame `my_sdf`, do
    `my_sdf.process(a_row)` and you will receive a result with all transformations
    applied. This can be done indefinitely, and new operations can be added at any time!

    Note there is also the concept of "filtering"; just like in Pandas, if you
    do an operation that would remove a row from a DataFrame, the same happens with
    StreamingDataFrames. Consequently, once a processing step nullifies the Row (usually
    by not returning a row with .apply, or returning False upon some sort of inequality
    check), all further processing will cease, returning None as the result.

    This means rows will NOT be produced to a topic if you "filter" them before they
    reach the "to_topic" step.

    There is a `Runner` class that helps facilitate the correct initialization of
    the kafka-related objects StreamingDataFrame uses, along with automating the
    consumption and processing (`StreamingDataFrame.process()`) of newly consumed
    messages with the StreamingDataFrame. It is thus recommended to hand your fully
    defined StreamingDataFrame reference to a `Runner` instance when doing things like
    accessing state, or interacting with kafka messages.

    Below are some simple examples of how you might use this (also see the `Runner`
    class for more details on how to properly hand a StreamingDataFrame instance to it):

    # Define your processing steps
    # Remove column_a, add 1 to columns b and c, skip row if b+1 >= 5, publish row
    df = StreamingDataframe()
    df = df[['column_b', 'column_c']]
    df = df.apply(lambda row: row[key] + 1 if key in ['column_b', 'column_c'])
    df = df[df['column_b'] + 1] >= 5]
    df.to_topic('my_output_topic')

    # Incomplete Rows to showcase what data you are actually interacting with
    record_0 = Row(value={'column_a': 'a_string', 'column_b': 3, 'column_c': 5})
    record_1 = Row(value={'column_a': 'a_string', 'column_b': 1, 'column_c': 10})

    # process records
    df.process(record_0) -> produces {'column_b': 4, 'column_c': 6} to "my_output_topic"
    df.process(record_1) -> filters row, does NOT produce to "my_output_topic"
    """
    def __init__(
        self, topics: List[Topic], _pipeline: Pipeline = None, _id: str = None
    ):
        self._id = _id or str(uuid.uuid4())
        self._pipeline = _pipeline or Pipeline(_id=self.id)
        self._real_consumer: Optional[RowConsumerProto] = None
        self._real_producer: Optional[RowProducerProto] = None
        if not topics:
            raise ValueError("Topic list cannot be empty")
        self._topics = {t.name: t for t in topics}

    def apply(
        self, func: Callable[[Row], Optional[Union[Row, list[Row], None]]]
    ) -> Self:
        """
        Add a callable to the StreamingDataframe execution list.
        The provided callable should accept a Quixstreams Row as its input.
        The provided callable should operate on and return the same input Row, or None
        if its intended to be a "filtering" function.

        :param func: callable that accepts and (usually) returns a QuixStreams Row
        :return: self (StreamingDataFrame)
        """
        self._pipeline.apply(func)
        return self

    def process(self, row: Row) -> Optional[Union[Row, list[Row]]]:
        """
        Execute the previously defined StreamingDataframe operations on a provided Row.
        :param row: a QuixStreams Row object
        :return: Row, list of Rows, or None (if filtered)
        """
        return self._pipeline.process(row)

    # TODO: maybe we should just allow list(Topics) as well (in many spots actually)
    def to_topic(self, topic: Topic):
        """
        Produce a row to a desired topic.
        Note that a producer must be assigned on the StreamingDataFrame if not using
        a QuixStreams `Runner` class to facilitate the execution of StreamingDataFrame.

        :param topic: A QuixStreams `Topic`
        :return: self (StreamingDataFrame)
        """
        return self.apply(lambda row: self._produce(topic, row))

    @property
    def id(self) -> str:
        return self._id

    @property
    def topics(self) -> Mapping[str, Topic]:
        """
        Get a mapping with Topics for the StreamingDataFrame
        :return: dict of {<topic_name>: <Topic>}
        """
        return self._topics

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

    def __setitem__(self, key: str, value: Union[Column, OpValue, str]):
        self.apply(lambda row: setitem(key, value, row))

    def __getitem__(
        self, item: Union[str, list[str], Column, Self]
    ) -> Union[Column, Self]:
        if isinstance(item, Column):
            return self.apply(lambda row: row if item.eval(row) else None)
        elif isinstance(item, list):
            return self.apply(lambda row: subset(item, row))
        elif isinstance(item, StreamingDataFrame):
            # TODO: Implement filtering based on another SDF
            raise ValueError(
                "Filtering based on StreamingDataFrame is not supported yet."
            )
        else:
            return Column(col_name=item)

    def _produce(self, topic: Topic, row: Row) -> Row:
        self.producer.produce_row(row, topic)
        return row
