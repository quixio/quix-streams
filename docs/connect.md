# Connect to Quix

The Quix SDK comes with a streaming client that enables you to connect
to Quix easily, to read data from Quix, and to write data to Quix. The
streaming client manages the connections between your application and
Quix and makes sure that the data is delivered reliably to and from your
application.

## Using QuixStreamingClient

Starting with 0.4.0, we’re offering QuixStreamingClient, which handles
the cumbersome part of setting up your streaming credentials using the
Quix Api. When you’re running the app in the [online
IDE](/platform/definitions/#online-ide) or as a
[deployment](/platform/definitions#deployment), all you have to
do is the following:


### Initialize the Client

=== "Python"
    
    ``` python
    client = QuixStreamingClient()
    ```

=== "C\#"
    
    ``` cs
    var client = new Quix.Sdk.Streaming.QuixStreamingClient();
    ```

If you wish to run the same code locally, you’ll have to provide an
OAuth2.0 bearer token. We have created a purpose made token for this,
called [SDK token](/platform/how-to/use-sdk-token). Once you have
the token you will have to provide it as an argument to
QuixStreamingClient or set
`Quix__Sdk__Token`
environment variable.

### Initialize the Client with an SDK Token

=== "Python"
    
    ``` python
    client = QuixStreamingClient('your_token')
    ```

=== "C\#"
    
    ``` cs
    var client = new Quix.Sdk.Streaming.QuixStreamingClient("your_token");
    ```

Using the streaming client is another way to talk with a broker. It
is a Kafka specific client implementation that requires some
explicit configuration but allows you to connect to any Kafka
cluster even outside Quix platform. It involves the following steps:

- Obtain a client certificate and credentials (security context) for your application.

- Create a streaming client.

A security context consists of a client certificate, username, and
password. Quix generates these automatically for you when you create
a project using the templates provided on the Quix Portal. If
necessary, you can download these credentials separately from Topics
option on Quix Portal.

The following code shows you how to set up the `SecurityOptions` for
your connection and how to create a `StreamingClient` instance to
start [Reading](/sdk/read.md) and [Writing](/sdk/write.md) real-time
time series data with Quix:
    
### Set Up the Security Options for Your Connection
    
=== "Python"
	
	``` python
	security = SecurityOptions(CERTIFICATES_FOLDER, QUIX_USER, QUIX_PASSWORD)
	client = StreamingClient('kafka-k1.quix.ai:9093,kafka-k2.quix.ai:9093,kafka-k3.quix.ai:9093', security)
	```

=== "C\#"
	
	``` cs
	var security = new SecurityOptions(CERTIFICATES_FOLDER, QUIX_USER, QUIX_PASSWORD);
	var client = new Quix.Sdk.Streaming.StreamingClient("kafka-k1.quix.ai:9093,kafka-k2.quix.ai:9093,kafka-k3.quix.ai:9093", security);
	```

=== "Javascript"

	Quix web APIs are secured with OAuth2.0 bearer scheme.
	Therefore, all HTTP requests to Quix must contain a valid bearer
	token. You can generate a personal access token (PAT) for use as
	a bearer token from the portal by following the following steps.
	
	  - Navigate to your profile by clicking on your avatar and
		selecting "Profile" from the drop-down menu.
	
	  - Select "Personal Access Tokens" tab on the profile page.
	
	  - Click on "Generate Token" button to open a dialog to set a
		name and an expiry date for the PAT and click on "Create" to
		generate the PAT.
  
   
    !!! tip
    
		For your convenience, when you create a new project on the Quix platform, the credentials are generated and set in the code for you. However, it is good practice to move them out of the code to a more secure location like environment variables or a keystore, depending on your development platform.
    
    When you deploy your application to Quix, you can store them on Quix
    as environment variables.
    
    
    


