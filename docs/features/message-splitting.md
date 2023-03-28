# Message splitting

Message brokers have message size limitation by design to ensure performance. For example, Kafka has a 1MB limit by default, and while you can increase it, it will have performance implications and has an upper suggested limit of 10 MB. This can be a problem in some use cases where messages published are big such as sound, video binary chunks or just huge volumes of time-series data.

Quix Streams automatically handles large messages on the producer side, splitting them up if required and merging them back together at the consumer side, in a totally transparent way.

![High level of splitting / merging flow](../images/QuixStreamsSplitting.png)

This feature gives you currently 255 times the broker message size limit, independent of the message broker used. This can be useful for intermittent messages which exceed the limit of your broker without getting exceptions.

!!! warning

    While this feature is undeniably useful, effort should be made to stay within your broker's limit to avoid complex commit limitations and increased memory footprint in the consumer. If you consistently have huge messages, should consider uploading to external storage and only sending a reference using the broker.
