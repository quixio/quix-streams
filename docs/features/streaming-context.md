# Streaming context

Using a plain Broker SDK out of box only enables you to send messages
independently, without any relationship between them.

![Read and Write using plain Broker SDK](../images/PlainBrokerMessaging.png)

The Quix SDK handles stream context for you, so all the data from one
data source is bundled in the same scope. This supports, among other
things, [automatic horizontal
scaling](#:features/horizontal-scaling.md) of your models when you
deal with a undertermined or big number of data sources.

![Horizontal scalability using Quix SDK](../images/QuixSdkScaling.png)

The SDK simplifies the processing of streams by providing callbacks on
the reading side. When processing stream data, you can identify data
from different streams more easily than with the key-value approach and
single messages used by other technologies.

The SDK also allows you to [attach
metadata](#write.adoc#_create_attach_to_a_stream) to streams, like ids,
references, or any other type of information related to the data source.

![Attach metadata to streams using Quix SDK](../images/QuixSdkMetadata.png)

This metadata can be read in real time by the SDK itself or via [the
Query API](#apis::data-catalogue-api/intro.md), if you choose to
persist the streams into the database.
