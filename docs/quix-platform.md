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


## Connecting with Quix Streams

Here is how to connect to the Quix Cloud Kafka brokers using Quix Streams:

### Within Quix Cloud

If you are running your Quix Streams `Application` directly within Quix Cloud, it's configured automatically.

### Outside of Quix Cloud


#### 1. Obtain Streaming Token
First, [get your Streaming
Token](https://quix.io/docs/develop/authentication/streaming-token.html#how-to-find) (also
called `SDK Token`).

> NOTE: A `Personal Access Token` (PAT) is also accepted, but often requires more configuration 
> due to raised privileges. We recommend using the `SDK Token`.

<br>

#### 2. Pass Streaming Token to Application
With your `SDK Token` and a Quix Streams `Application`, you can pass it using one of
these approaches:

- **Set Environment Variable** (***recommended***):
    - Simply set `Quix__Sdk__Token` (double underscores!) to your `SDK Token`
  > NOTE: `Quix__Sdk__Token` is set automatically in Quix Cloud, thus is the recommended approach for an easy migration.
  
OR <br>

- **Application argument** 
    - Just do `Application(quix_sdk_token="MY_TOKEN")` 
  > WARNING: This value is prioritized over `Quix__Sdk__Token`, which may cause ambiguity if both are set.

<br>

#### 3. Validate it Worked! 
When you initialize your `Application()`, a message should be logged letting 
you know it will connect to Quix Cloud when `Application.run()` is called. 

If not, you'll need to double check your settings and try again!