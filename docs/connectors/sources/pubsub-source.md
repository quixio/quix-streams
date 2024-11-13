# Google Cloud (GCP) Pub/Sub Source

!!! info

    This is a **Community** connector. Test it before using in production.

    To learn more about differences between Core and Community connectors, see the [Community and Core Connectors](../community-and-core.md) page.

This source enables reading from a Google Cloud (GCP) Pub/Sub topic, dumping it to a
kafka topic using desired `StreamingDataFrame`-based transformations.

## How to use Google Cloud Pub/Sub Source

To use a GCP PubSub Source, you need two components:

1. `GoogleCloudCredentials` for Google Cloud client authentication details.
2. `PubSubSource` (which uses the `GoogleCloudCredentials`); hand it to `app.dataframe()`.

For the full description of expected parameters of each, see the [GCP PubSub Source API](../../api-reference/sources.md#gcppubsubsource) page.  

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

## Message data format/schema

Currently, forwarding message keys (also known as "ordered messages" with Google Cloud Pub/Sub) is unsupported.

The incoming message value will be in bytes, so transform accordingly in your SDF directly.

## Source Processing Guarantees

The Pub/Sub Source offers "at-least-once" guarantees: there is no confirmation that
message acknowledgements for the Pub/Sub Subscriber succeeded.

As such, in rare circumstances where acknowledgement ends up failing, messages may be 
processed (produced) more than once (and additionally, out of their original order).
    
## Topic

The default topic name the Application dumps to is `gcp-pubsub_{subscription_name}_{topic_name}`.
