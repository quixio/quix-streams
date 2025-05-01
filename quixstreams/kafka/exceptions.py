from confluent_kafka import KafkaError

from quixstreams.exceptions import QuixException


class BaseKafkaException(QuixException):
    def __init__(self, error: KafkaError):
        self.error = error

    @property
    def code(self) -> int:
        return self.error.code()

    @property
    def description(self):
        return self.error.str()

    def __str__(self):
        return (
            f"<{self.__class__.__name__} "
            f'code="{self.code}" '
            f'description="{self.description}">'
        )

    def __repr__(self):
        return str(self)


class KafkaConsumerException(BaseKafkaException): ...


class KafkaProducerDeliveryError(BaseKafkaException): ...


class InvalidProducerConfigError(QuixException): ...
