import logging
import time
from collections import Counter
from random import choice

from quixstreams.sources import Source

logger = logging.getLogger(__name__)


class ReviewGenerator(Source):
    _review_list = [
        "This is the best thing since sliced bread. The best I say.",
        "This is terrible. Could not get it working.",
        "I was paid to write this. Seems good.",
        "Great product. Would recommend.",
        "Not sure who this is for. Seems like it will break after 5 minutes.",
        "I bought their competitors product and it is way worse. Use this one instead.",
        "I would buy it again. In fact I would buy several of them.",
        "Great great GREAT",
    ]

    _product_list = ["product_a", "product_b", "product_c"]

    def __init__(self):
        super().__init__(name="customer-reviews")

    def run(self):
        for review in self._review_list:
            event = self.serialize(key=choice(self._product_list), value=review)
            logger.debug(f"Generating review for {event.key}: {event.value}")
            self.produce(key=event.key, value=event.value)
            time.sleep(0.5)  # just to make things easier to follow along
        logger.info("Sent all product reviews.")


def setup_and_run_application():
    """Group all Application-related code here for easy reading."""
    import os

    from quixstreams import Application

    app = Application(
        broker_address=os.getenv("BROKER_ADDRESS", "localhost:9092"),
        consumer_group="product_review_word_counter",
        auto_offset_reset="earliest",
    )
    word_counts_topic = app.topic(name="product_review_word_counts")

    def tokenize_and_count(text):
        return list(Counter(text.lower().replace(".", " ").split()).items())

    def should_skip(word_count_pair):
        word, count = word_count_pair
        return word not in ["i", "a", "we", "it", "is", "and", "or", "the"]

    # If reading from a Kafka topic, pass topic=<Topic> instead of a source
    sdf = app.dataframe(source=ReviewGenerator())
    sdf = sdf.apply(tokenize_and_count, expand=True)
    sdf = sdf.filter(should_skip)
    # .to_topic() does not require reassignment ("in-place" operation), but does no harm
    sdf = sdf.to_topic(
        word_counts_topic, key=lambda word_count_pair: word_count_pair[0]
    )


# This approach is necessary since we are using a Source
if __name__ == "__main__":
    setup_and_run_application()
