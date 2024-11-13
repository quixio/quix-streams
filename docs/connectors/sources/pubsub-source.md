# Google Cloud (GCP) Pub/Sub Source

!!! info

    This is a **Community** connector. Test it before using in production.

    To learn more about differences between Core and Community connectors, see the [Community and Core Connectors](../community-and-core.md) page.

This source enables reading from a Google Cloud (GCP) Pub/Sub topic, dumping it to a
kafka topic using desired `StreamingDataFrame`-based transformations.

## How to use Google Cloud Pub/Sub Source

To use Pub/Sub Source, hand `PubSubSource` to `app.dataframe()`.

For the full description of expected parameters of each, see the [PubSub Source API](../../api-reference/sources.md#pubsubsource) page.  

```python
from quixstreams import Application
from quixstreams.sources.community.pubsub import PubSubSource

source = PubSubSource(
    service_account_json='{"my": "creds"}',
    project_id="my_project",
    topic_id="pubsub_topic_name",  # NOTE: NOT the full GCP path!
    subscription_id="pubsub_topic_name",  # NOTE: NOT the full GCP path!
    create_subscription=True,
)
app = Application(
    broker_address="localhost:9092",
    auto_offset_reset="earliest",
    consumer_group="gcp",
    loglevel="DEBUG"
)
sdf = app.dataframe(source=source).print(metadata=True)

if __name__ == "__main__":
    app.run()
```

## Testing locally

Rather than connect to Google Cloud, you can alternatively test your application using 
a local "emulated" Pub/Sub host via docker:

1. DO NOT pass a `service_account_json` to `PubSubSource`, instead set environment variable:
    
    `PUBSUB_EMULATOR_HOST=localhost:8085`

2. execute in terminal:

    `docker run -d --name pubsub-emulator -p 8085:8085 gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators gcloud beta emulators pubsub start --host-port=0.0.0.0:8085`

## Message ordering (keys)

Currently, forwarding message keys (also known as "ordered messages" with in Pub/Sub) 
is only supported when the subscription is NOT created by the Pub/Sub Source.

## Message data format/schema

Incoming message keys will be strings (non-ordered messages will be empty strings).

Incoming message values will be in bytes, so transform accordingly in your SDF directly.

## Source Processing Guarantees

The Pub/Sub Source offers "at-least-once" guarantees: there is no confirmation that
message acknowledgements for the Pub/Sub Subscriber succeeded.

As such, in rare circumstances where acknowledgement ends up failing, messages may be 
processed (produced) more than once (and additionally, out of their original order).
    
## Topic

The default topic name the Application dumps to is `gcp-pubsub_{subscription_name}_{topic_name}`.
