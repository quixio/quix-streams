# Quix Environment Source

A specialised [Kafka Source](kafka-source.md) that simplify copying data from a Quix environment.

## How to use the Quix Environment Source

To use a Quix Environment source, you need to create an instance of `QuixEnvironmentSource` and pass it to the `app.dataframe()` method.

```python
from quixstreams import Application
from quixstreams.sources import QuixEnvironmentSource

def main():
  app = Application()
  source = QuixEnvironmentSource(
    name="my-source",
    app_config=app.config,
    topic="source-topic",
    quix_sdk_token="quix-sdk-token",
    quix_workspace_id="quix-workspace-id",
  )

  sdf = app.dataframe(source=source)
  sdf.print(metadata=True)

  app.run(sdf)

if __name__ == "__main__":
  main()
```

## Token

The Quix Environment Source requires the sdk token of the source environment. [Click here](../../../develop/authentication/streaming-token.md) for more information on SDK tokens.
