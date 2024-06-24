from typing import Protocol, Literal, Union

ExactlyOnceSemantics = Literal["exactly-once", "EO", "EOS"]
AtLeastOnceSemantics = Literal["at-least-once", "ALO", "ALOS"]
ProcessingGuarantee = Literal[Union[ExactlyOnceSemantics, AtLeastOnceSemantics]]


class TopicPartition(Protocol):
    topic: str
    partition: int
    offset: int
