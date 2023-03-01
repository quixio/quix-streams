# App management

In order to reduce the amount of boilerplate code added to each of our samples, we developed the `App.run()` feature.

`App.run` takes care of several small but important tasks in managing your Python apps. These include:

* Start subscribing to topics
* Handle termination
* Close streams and dispose topics
* Keep alive

Each of these is described in the following sections.

## Imports

To use `App.run()` in your code, you will need this import:

```py
from quixstreams import App
App.run()
```

## Start subscribing

In order to start receiving from a topic you need to make a call to the `subscribe()` method. Your Python code won’t be able to receive any data from the broker if you have missed this step. It makes sense to add this call near the end of the code, just before your 'keep busy' `while` loop.

Your code might look something like this:

```py
from quixstreams import TopicConsumer, StreamConsumer

def on_stream_received_handler(stream_received: StreamConsumer):
    buffer = stream_received.timeseries.create_buffer()
    buffer.on_dataframe_released = on_dataframe_released_handler

def on_dataframe_released_handler(stream: StreamConsumer, df: pd.DataFrame):
    print(df.to_string())

# ... some code setting up topic_consumer
topic_consumer.on_stream_received = on_stream_received_handler
topic_consumer.subscribe()

while(True):  # or other blocking call
    print('running')
```

However, using `App.run()` you no longer need the call to `subscribe()`, because it is called for you. 

So your code would look like this instead:

```py
from quixstreams import App, TopicConsumer, StreamConsumer
import pandas as pd

def on_stream_received_handler(stream_received: StreamConsumer):
    buffer = stream_received.timeseries.create_buffer()
    buffer.on_dataframe_released = on_dataframe_released_handler

def on_dataframe_released_handler(stream: StreamConsumer, df: pd.DataFrame):
    print(df.to_string())

# ... some code setting up topic_consumer
topic_consumer.on_stream_received = on_stream_received_handler

App.run()
```

## Termination

Termination signals such as `SIGINT`, `SIGTERM` and `SIGQUIT` could be used to break out of the `while` loop used to keep your code listening for data. In order to listen for these you’d need to add some code like this:

```py
import threading
import signal
import time

# Handle graceful exit
event = threading.Event() 

def signal_handler(sig, frame):
    # set the termination flag
    print('Setting termination flag')
    event.set()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

while not event.is_set():
    time.sleep(1)

print('Exiting')
```

In this case, when the code runs, it will subscribe to and handle `SIGINT` (an interrupt signal) and `SIGTERM` (a termination signal). If either of these signals is observed, the code will call `signal_handler`, and the `while` loop will terminate allowing the code execution to come to an end.

`App.run` has this termination signal handling built in. It works on all popular platforms too.

Using `App.run` the above code becomes much simpler:

```py
from quixstreams import App
App.run()
print('Exiting')
```

## Close streams

Ideally, when your code terminates it will close the streams it opened and tidy up any other resources it was using.

You can ensure the streams are closed by calling the following code just before your process terminates:

```py
# dispose the topic(s) and close the stream(s)
print('Closing streams...')
topic_consumer.dispose()  # Note the order. Producer should be closed after consumer
topic_producer.dispose()  # to avoid receiving data - possibly committing - but not being able to write.
print('Closed streams...')

# or using the with statement, if your code structure is suited for it
with topic_producer, topic_consumer:  # disposed in reverse order
    pass  # do great stuff
# disposed when going out of scope
print('Closed streams...')
```

Here, you dispose of the topic consumer, which stops new data from being received, and then dispose of the topic producer. This code is easy and straightforward, however it is just more boilerplate that you have to remember.

Once again, `App.run()` encapsulates this and handles this for you, so the code above becomes:

```py
from quixstreams import App
App.run()
```

## Keep alive

Unless you add an infinite loop or similar code, a Python code file will run each code statement sequentially until the end of the file, and then exit.  

In order to continuously handle data in your Python code, you need to prevent the code from terminating. There are several ways this could be achieved. For example, you could use an infinite `while` loop to allow your code to run continuously. The following code will continuously print "running" until the `end_condition` has been satisfied:

```py
run = True
while(run):
    print('running')
    if(end_condition = True):
        run = False
print('ending')
```

We used code similar to this in our [Quix Library](https://github.com/quixai/quix-library) items for a while. However, once we'd seen the same pattern being used repeatedly, we decided to build this functionality into Quix Streams.

This is how you can use it in your code:

```py
from quixstreams import App
App.run()
```

# Bring it all together

You have seen how you could start subscribing to streams, handle termination signals, dispose of topics consumers and producers, and how to keep your code running. 

To recap, here is an example of your code without using `App.run`, all in one snippet:

```py
import threading
import signal
import time

# ... some code setting up topic_consumer and topic_producer
topic_consumer.subscribe()  # initiate read

# Hook up to termination signal (for docker image) and CTRL-C
print('Listening to streams. Press CTRL-C to exit.')

# Below code is to handle graceful exit of the model.
event = threading.Event() 

def signal_handler(sig, frame):
    # dispose the topic(s) and close the stream(s)
    print('Closing streams...')
    topic_consumer.dispose()
    topic_producer.dispose()

    print('Setting termination flag')
    event.set()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

while not event.is_set():
    time.sleep(1)

print('Exiting')
```

If you use `App.run()`, this is greatly simplified into the following much smaller snippet:

```py
from quixstreams import App
# run a while loop
# Subscribe to the input topics
# listen for termination
# close topic consumers and producers
App.run()
print('Exiting')
```

## Before shutdown

It's good practice to make sure that your code cleans up during the shutdown phase. Cleanup includes disposing of any resources you might have used, or indicating to external systems that they need to close or deallocate resources.

If you choose to implement the cleanup of other resources, or simply need to log something immediately before the code ends, you can configure `App.run()` to call a function before it does all of the other built-in actions. `App.run` provides the `before_shutdown` hook to enable this facility. The following code provides an example of this:

```py
from quixstreams import App
def before_shutdown():
    print('before shutdown')

App.run(before_shutdown=before_shutdown)
```

In this snippet, `before_shutdown` is called before the app shuts down. That is before the `while` loop inside `App.run` comes to an end. This allows you to close connections, tidy up, and log your last messages before the app terminates.

## Triggering shutdown from code

In some cases you might want to trigger shutdown from code. You can do it using `CancellationTokenSource`.

```py
from quixstreams import App, CancellationTokenSource
import threading
import time

cts = CancellationTokenSource()  # used for interrupting the App

# setup shutdown after 5 seconds
def timeout_callback():
    time.sleep(5)
    cts.cancel()

timeout_thread = threading.Thread(target=timeout_callback)
timeout_thread.start()

# Setup some prints and pass the token to App.run
def before_shutdown():
    print('before shutdown')

print('Waiting 5 seconds')
App.run(cts.token, before_shutdown=before_shutdown)
print('exiting')
```