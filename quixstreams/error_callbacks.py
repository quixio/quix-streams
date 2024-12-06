import logging
from typing import Callable, Optional

from .models import RawConfluentKafkaMessageProto, Row

ProcessingErrorCallback = Callable[[Exception, Optional[Row], logging.Logger], bool]
ConsumerErrorCallback = Callable[
    [Exception, Optional[RawConfluentKafkaMessageProto], logging.Logger], bool
]
ProducerErrorCallback = Callable[[Exception, Optional[Row], logging.Logger], bool]


def default_on_consumer_error(
    exc: Exception,
    message: Optional[RawConfluentKafkaMessageProto],
    logger: logging.Logger,
):
    topic, partition, offset = None, None, None
    if message is not None:
        topic, partition, offset = (
            message.topic(),
            message.partition(),
            message.offset(),
        )
    logger.exception(
        f"Failed to consume a message from Kafka: "
        f'partition="{topic}[{partition}]" offset="{offset}"',
    )
    return False


def default_on_processing_error(
    exc: Exception, row: Row, logger: logging.Logger
) -> bool:
    logger.exception(
        f"Failed to process a Row: "
        f'partition="{row.topic}[{row.partition}]" offset="{row.offset}"',
    )
    return False


def default_on_producer_error(
    exc: Exception, row: Optional[Row], logger: logging.Logger
) -> bool:
    topic, partition, offset = None, None, None
    if row is not None:
        topic, partition, offset = row.topic, row.partition, row.offset
    logger.exception(
        f"Failed to produce a message to Kafka: "
        f'partition="{topic}[{partition}]" offset="{offset}"',
    )
    return False
