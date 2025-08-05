# Connecting to Quix Cloud

### Why Use Quix Cloud
Quix Streams was made to seamlessly integrate with the Quix Cloud.

When running Quix Streams on Quix Cloud, all connection and authentication-based 
settings are preconfigured for you.

Quix Cloud itself provides a frictionless environment to deploy and manage your applications using either
our API or our browser-based UI.

To learn more about Quix Cloud and how to set up a project, please see the [Quix Cloud docs](https://quix.io/docs/quix-cloud/overview.html#developing-your-stream-processing-application).

### Can I Use Quix Streams With My Own Kafka?

Using Quix Streams with Quix Cloud is entirely optional.  

You could, for example, connect to Redpanda Cloud, or another supported broker, or connect to a self-hosted broker.


### Can I Use a Schema Registry Alongside Quix Cloud?

Assuming you have a Schema Registry instance, you can use it alongside Quix Cloud using 
Quix Streams.

Currently, Quix Cloud does not offer a Schema Registry or support configuring one directly, 
which means it does not support visualizing messages serialized using one. Everything 
else should function as expected.

[Learn how to connect to a Schema Registry here](./advanced/schema-registry.md).

## Connecting with Quix Streams

Here is how to connect to the Quix Cloud Kafka brokers using Quix Streams:

### Within Quix Cloud

If you are running your Quix Streams `Application` directly within Quix Cloud, it's configured automatically.

Just make sure `broker_address` is NOT set:

```python
from quixstreams import Application

app = Application()
```
OR
```python
from quixstreams import Application

app = Application(broker_address=None)
```

### Outside of Quix Cloud


#### 1. Obtain SDK Token and Quix Portal API URL
First, [get your SDK Token](https://quix.io/docs/develop/authentication/streaming-token.html#how-to-find).

The **Quix Portal API URL** can be found on the "Settings > API and Tokens" tab inside Quix Platform.

> NOTE: A `Personal Access Token` (PAT) is also accepted, but often requires more configuration 
> due to raised privileges. We recommend using the `SDK Token`.

<br>

#### 2. Pass SDK Token and Portal API URL to the Application
With your `SDK Token` and a Quix Streams `Application`, you can pass it using one of
these approaches:

- **Set Environment Variables** (***recommended***):
    - Set `Quix__Sdk__Token` (double underscores!) to your `SDK Token`
    - Set `Quix__Portal__Api` to the URL obtained from your Quix Platform project settings. 

  > NOTE: `Quix__Sdk__Token` and `Quix__Portal__Api` are set automatically in Quix Cloud and by [Quix CLI](https://quix.io/docs/quix-cli/overview.html).
  
OR <br>

- **Application argument** 
    - Just do:
      ```python
      from quixstreams import Application
      
      app = Application(
          quix_sdk_token="MY_TOKEN",
          quix_portal_api="https://portal.cloud.quix.io/",
      )
      ```
  > WARNING: These values are prioritized over `Quix__Sdk__Token` and `Quix__Portal__Api`, which may cause ambiguity if both are set.

<br>

#### 3. Validate it Worked! 
When you initialize your `Application()`, a message should be logged letting 
you know it will connect to Quix Cloud when `Application.run()` is called. 

If not, you'll need to double check your settings and try again!

## Local Development to Quix Cloud

For those wanting to [develop with a local broker](tutorials/README.md#running-kafka-locally) 
and then migrate to Quix Cloud, here is a recommended example pattern that avoids code changes:

```python
from quixstreams import Application
import os

app = Application(broker_address=os.getenv("YOUR_ENV_VAR", None))
```

Just pick an environment variable and set it to your local broker address!

In Quix Cloud it will default to `None` as desired and use the Quix Cloud environment.