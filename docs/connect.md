# Connecting to a broker

It is possible to connect Quix Streams to brokers such as:

* Kafka
* Quix Platform (which also uses Kafka)

Connecting Quix Streams to these brokers is discussed in the following sections.

For more information on how Quix Streams works with Kafka, read [Kafka and Quix Streams](kafka.md).

## Connecting to Kafka

Connect to Kafka using the `KafkaStreamingClient` class provided by the library. This is a Kafka specific client implementation that requires some explicit configuration but allows you to connect to any Kafka cluster even outside Quix Platform.

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

If your broker is secured, the library provides easy authentication when using username and password with an optional certificate to validate server identity. 

The following code shows you how to set up the `SecurityOptions` for your connection and how to create a `KafkaStreamingClient` instance to start [subscribing](subscribe.md) to topics and [publishing](publish.md) time series data:
        
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

Quix Streams comes with a streaming client that enables you to connect to Quix SaaS topics easily. The streaming client manages the connections between your application and Quix and makes sure that the data is delivered reliably to and from your application. You can still use `KafkaStreamingClient` and manually set details, but `QuixStreamingClient` is a much easier way to connect.

`QuixStreamingClient` handles the cumbersome part of setting up your streaming credentials using the Quix API. 

### Code running in Quix Platform

When you’re running the app in our online IDE or as a Quix deployment, the following code connects to Quix:

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

If you wish to connect to Quix with your code running locally, you’ll have to provide an OAuth2.0 bearer token. Quix have created a token for this, called `SDK token`. 

Once you have [obtained the token](../platform/how-to/use-sdk-token.md) you will have to provide it as an argument to `QuixStreamingClient` or set `Quix__Sdk__Token` environment variable. This is shown in the following code:

=== "Python"
    
    ``` python
    from quixstreams import QuixStreamingClient
    
    client = QuixStreamingClient('your_token')
    ```

=== "C\#"
    
    ``` cs
    var client = new QuixStreams.Streaming.QuixStreamingClient("your_token");
    ```