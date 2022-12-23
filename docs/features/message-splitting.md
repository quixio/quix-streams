# Message splitting

Message Brokers always have some limitations, by design, with the
maximum size of message they can handle. For example, Kafka has a 1MB
limit by default, and it’s not recommended to increase this number, for
performance reasons. This can be a problem in some use cases where
time-series data are big binary chunks, such as sound or video.

The Quix SDK automatically handles large messages on the producer side,
splitting them up if required and merging them back together at the
consumer side, in a totally transparent way.

![High level of splitting / merging flow](../images/QuixSdkSplitting.png)

This feature gives you an unlimited message size, independent of the
Message Broker used.
