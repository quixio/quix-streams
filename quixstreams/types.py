from typing import Protocol, Optional, List, Tuple, Dict, Union


class TopicPartition(Protocol):
    topic: str
    partition: int
    offset: int


HeaderValue = Optional[Union[str, bytes]]
Headers = Union[
    List[Tuple[str, HeaderValue]],
    Dict[str, HeaderValue],
]
