# Google Cloud Product (GCP) PubSub Source

!!! info

    This is a **Community** connector. Test it before using in production.

    To learn more about differences between Core and Community connectors, see the [Community and Core Connectors](../community-and-core.md) page.

This source enables reading from a Google Cloud Platform (GCP) PubSub topic, dumping it to a
kafka topic using desired SDF-based transformations.

## How to use GCP PubSub Source

To use a GCP PubSub Source, you need two components:

1. `GCPPubSubConfig` for Google Cloud Platform client connection details
2. `GCPPubSubSource` (which uses the `GCPPubSubConfig`); hand it to `app.dataframe()`.

For the full description of expected parameters of each, see the [GCP PubSub Source API](../../api-reference/sources.md#gcppubsubsource) page.  

```python
from quixstreams import Application
from quixstreams.sources.community.gcp_pubsub import GCPPubSubConfig, GCPPubSubSource

# for authorization
config = GCPPubSubConfig(
    credentials_path="/path/to/google/auth/creds.json"
)
source = GCPPubSubSource(
    config=config,
    project_id="my_gcp_project",
    topic_name="my_gcp_pubsub_topic_name",  # NOTE: NOT the full GCP path!
    subscription_name="my_gcp_pubsub_topic_name",  # NOTE: NOT the full GCP path!
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

Rather than connect to GCP, you can alternatively test your application using a local
"emulated" GCP PubSub host via docker:

1. Use this `GCPPubSubConfig` (do NOT set the `credentials_path`)
  `config = GCPPubSubConfig(emulated_host_url="localhost:8085")`
2. execute in terminal: 
  `docker run -d --name pubsub-emulator -p 8085:8085 gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators gcloud beta emulators pubsub start --host-port=0.0.0.0:8085`

## Message data format/schema

Currently, forwarding message keys (also known as "ordered messages" in GCP) is unsupported.

The incoming message value will be in bytes, so transform accordingly in your SDF directly.

## Source Processing Guarantees

The GCP PubSub Source offers "at-least-once" guarantees: there is no confirmation that
message acknowledgements for the GCP PubSub Subscriber succeeded.

As such, in rare circumstances where acknowledgement ends up failing, messages may be 
processed (produced) more than once (and additionally, out of their original order).
    
## Topic

The default topic name the Application dumps to is `gcp-pubsub_{subscription_name}_{topic_name}`.
