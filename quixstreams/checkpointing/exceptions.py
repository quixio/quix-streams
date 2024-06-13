from quixstreams.exceptions import QuixException
from quixstreams.kafka.exceptions import KafkaConsumerException


class InvalidStoredOffset(QuixException): ...


class CheckpointProducerTimeout(QuixException): ...


class CheckpointConsumerCommitError(KafkaConsumerException): ...
