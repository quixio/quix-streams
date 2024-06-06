from quixstreams.exceptions import QuixException


class InvalidStoredOffset(QuixException): ...


class CheckpointProducerTimeout(QuixException): ...
