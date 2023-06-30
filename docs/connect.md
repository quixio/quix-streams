# Connecting to a broker

It is possible to connect Quix Streams to a Kafka broker managed by the Quix Platform, or to your own managed or self-hosted Kafka broker.

How you can connect Quix Streams to these is discussed in the following sections.

## Connecting to Kafka

You can connect to Kafka using the `KafkaStreamingClient` class provided by the library. This is a Kafka specific client implementation that requires some explicit configuration, but enables you to connect to any Kafka cluster even outside of the Quix Platform.

When your broker requires no authentication, you can use the following code to create a client to connect to it:

=== "Python"
	
	``` python
    from quixstreams import KafkaStreamingClient

	client = KafkaStreamingClient('127.0.0.1:9092')
	```

=== "C\#"
	
	``` cs
	var client = new QuixStreams.Streaming.KafkaStreamingClient("127.0.0.1:9092");
	```

If your broker is secured, the library provides easy authentication when using username and password, with an optional certificate to validate server identity. 

The following code shows you how to set up the `SecurityOptions` for your connection, and how to create a `KafkaStreamingClient` instance to [subscribe](subscribe.md) to topics and [publish](publish.md) data:
        
=== "Python"
	
	``` python
    from quixstreams import SecurityOptions, KafkaStreamingClient

	security = SecurityOptions(CERTIFICATES_FOLDER, QUIX_USER, QUIX_PASSWORD)
	client = KafkaStreamingClient('127.0.0.1:9093', security)  # additional details can be set using `properties=`
	```

=== "C\#"
	
	``` cs
	var security = new SecurityOptions(CERTIFICATES_FOLDER, QUIX_USER, QUIX_PASSWORD);
	var client = new QuixStreams.Streaming.KafkaStreamingClient("127.0.0.1:9093", security);
	```

## Connecting to Quix

Quix Streams comes with a streaming client that enables you to connect to Quix Platform topics easily. The streaming client manages the connections between your application and Quix and makes sure that the data is delivered reliably to and from your application. You can still use `KafkaStreamingClient` and manually set details, but `QuixStreamingClient` is a much easier way to connect.

`QuixStreamingClient` handles the cumbersome part of setting up your streaming credentials using the Quix API. 

### Code running in Quix Platform

When you’re running the app in the Quix Portal, the following code connects to Quix:

=== "Python"
    
    ``` python
    from quixstreams import QuixStreamingClient

    client = QuixStreamingClient()
    ```

=== "C\#"
    
    ``` cs
    var client = new QuixStreams.Streaming.QuixStreamingClient();
    ```

### Code running locally

If you wish to connect to Quix with your code running locally, you’ll have to provide an **OAuth2.0** bearer token. Quix have created a token for this, called `SDK token`. 

Once you have [obtained the token](https://quix.io/docs/platform/how-to/streaming-token.html) you will have to provide it as an argument to `QuixStreamingClient` or set the `Quix__Sdk__Token` environment variable. This is shown in the following code:

=== "Python"
    
    ``` python
    from quixstreams import QuixStreamingClient
    
    client = QuixStreamingClient('your_token')
    ```

=== "C\#"
    
    ``` cs
    var client = new QuixStreams.Streaming.QuixStreamingClient("your_token");
    ```

## Next steps

For more information on how Quix Streams leverages the Kafka technology, read [Kafka and Quix Streams](kafka.md).
