import os
from collections import Counter

from quixstreams import Application

app = Application(
    broker_address=os.environ.get("BROKER_ADDRESS", "localhost:9092"),
    consumer_group="product_review_word_counter",
    auto_offset_reset="earliest",
)
product_reviews_topic = app.topic(name="product_reviews")
word_counts_topic = app.topic(name="product_review_word_counts")


def tokenize_and_count(text):
    return list(Counter(text.lower().replace(".", " ").split()).items())


def should_skip(word_count_pair):
    word, count = word_count_pair
    return word not in ["i", "a", "we", "it", "is", "and", "or", "the"]


sdf = app.dataframe(topic=product_reviews_topic)
sdf = sdf.apply(tokenize_and_count, expand=True)
sdf = sdf.filter(should_skip)
sdf = sdf.to_topic(word_counts_topic, key=lambda word_count_pair: word_count_pair[0])


if __name__ == "__main__":
    app.run(sdf)
