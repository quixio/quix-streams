# Connecting to Quix Cloud

### Why to Use Quix Cloud?
Quix Streams provide an API to seamlessly work with Quix Cloud platform.

You may use Quix Cloud to deploy and manage stream processing pipelines in a frictionless environment.

To learn more about Quix Cloud and how to set up a project, please see the [Quix Cloud docs](https://quix.io/docs/quix-cloud/overview.html#developing-your-stream-processing-application).

### Can I Use Quix Streams With My Own Kafka?

Using Quix Cloud with Quix Streams is optional.


## Connecting to Kafka brokers in Quix

To connect to the Kafka broker in Quix Cloud, you need to take the following steps:

**1. Create an Application instance using a special factory method - `Application.Quix()`.**    


It will configure the Application to connect to the Kafka broker in Quix Cloud using the Streaming Token provided via env variable `Quix__Sdk__Token`.

It has the same API as the `Application` class constructor, except it doesn't require you to pass the `broker_address`.

**2. [Obtain the Streaming Token for your Quix Cloud workspace](https://quix.io/docs/develop/authentication/streaming-token.html#how-to-find).**


**3. Pass the Streaming Token via env variable `Quix__Sdk__Token`.**

>***NOTE:*** When running in Quix Cloud, `Quix__Sdk__Token` environment variable is provided automatically.



**Example**:

Create a Quix Streams application and connect it to Quix Cloud.

Application code:

```python
# app.py
from quixstreams import Application

# Create an Application configured to use a Quix Cloud broker
# You may pass additional settings to it similar to usual Application init.
app = Application.Quix(auto_offset_reset='earliest', consumer_group='my-consumer-group')

# Define an input topic
topic = app.topic('input')

# Print all incoming messages
sdf = app.dataframe(topic).update(print)

# Run the Application
app.run(sdf)
```

Running the application:

```commandline
Quix__Sdk__Token=<my-sdk-token> python app.py
```
