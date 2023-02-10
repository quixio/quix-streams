# App management

In order to reduce the amount of boilerplate code we added to each of our samples, we developed the `App.run()` feature.

App.run takes case of several small but important tasks in our Python apps.

These are:

* Start reading from input topics
* Handle termination
* Close streams and dispose topics
* Keep alive

## Imports

To use `App.run()` in your code, you will need this  import:

```py
from quixstreams.app import App
```

## Start reading

In order to start reading from an input topic you need to make a call to the `start_reading()` method. Your Python code won’t be able to read any data from the broker if you have missed this step. It makes sense to add this call near the end of the code, just before your 'keep busy' 'while' loop.

Your code might look something like this:

```py
def read_stream(new_stream: StreamReader):

    buffer = new_stream.parameters.create_buffer()

    def on_pandas_frame_handler(df: pd.DataFrame):
        print(df.to_string())

    buffer.on_read_pandas += on_pandas_frame_handler

input_topic.on_stream_received += read_stream
input_topic.start_reading()

while(True)
    print(“running”)
```

However, using `App.run()` you can skip this call because it's built in. 

So your code would look like this instead:

```py
def read_stream(new_stream: StreamReader):

    buffer = new_stream.parameters.create_buffer()

    def on_pandas_frame_handler(df: pd.DataFrame):
        print(df.to_string())

    buffer.on_read_pandas += on_pandas_frame_handler

input_topic.on_stream_received += read_stream

App.run()
```

## Termination

Termination signals such as SIGINT, SIGTERM and SIGQUIT could be used to break out of the 'while' loop used to keep your code listening for data. In order to listen for these you’d need to add some code like this:

```py
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

In this case, when the code runs it will subscribe to and handle SIGINT (an interrupt signal) and SIGTERM (a termination signal). If either of these is observed the code will call `signal_handler` and the while loop will terminate allowing the code execution to come to an end.

App.run has this termination signal handling built in. It works on all popular platforms too.

Using App.run the above code becomes much simpler:

```py
App.run()
print('Exiting')
```

## Close streams

Ideally, when your code terminates it will close the streams it opened and tidy up any other resources it was using.

You can ensure the streams are closed by calling the following code just before your process terminates:

```py
# dispose the topic(s) and close the stream(s)
print('Closing streams...')
input_topic.dispose()
output_topic.dispose()
```

Here, you dispose of the input topic, which stops new data from being received and then dispose of the output topic. This code is easy and straightforward, however it's just more boilerplate that you have to remember.

Once again, `App.run()` encapsulates this and takes it off your todo list:

```py
App.run()
```

## Keep alive

Unless we add an infinite loop or some code to prevent it, a Python code file will usually run each code statement then end.  

In order to continuously handle data in our Python code we need to prevent the code file from terminating. There are several ways this could be achieved, for example we could use an infinite while loop to allow our code to run continuously. 

For example, this code will continuously print "running" until the `end_condition` has been satisfied:

```py
run = True
while(run):
    print(“running”)
    if(end_condition = True):
        run = False
print(“ending”)
```

We used code very similar to this in our Quix Library items for quite a while. However, once we'd seen the same pattern being used over and over we decided to build this functionality into QuixStreams.

This is how you can use it in your code:

```py
App.run()
```

# Bring it all together

You have seen how you could start reading from streams, handle termination signals, dispose of input and output topics and how to keep your code running. 

To recap, here is the long form way to do it all in one snippet:

```py
input_topic.start_reading()  # initiate read

# Hook up to termination signal (for docker image) and CTRL-C
print("Listening to streams. Press CTRL-C to exit.")

# Below code is to handle graceful exit of the model.
event = threading.Event() 

def signal_handler(sig, frame):
    # dispose the topic(s) and close the stream(s)
    print('Closing streams...')
    input_topic.dispose()
    output_topic.dispose()

    print('Setting termination flag')
    event.set()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

while not event.is_set():
    time.sleep(1)

print('Exiting')
```

If you use `App.run()` this is greatly simplified into the following much smaller snippet:

```py
# run a while loop
# open the input topics
# listen for termination
# close input and output topics
App.run()
print('Exiting')
```

## Before shutdown

Before we shutdown this page on `App.run()` we want revisit disposing of resources your app might have used. It's good practice to clean up after yourself! Dispose of any resources you might have used or tell external systems to close or de-allocate resources.

If you choose to implement the cleanup of other resources or simply need to log something immediately before the code ends, you can configure `App.run()` to call a function before it does all of the other built in actions.

```py
def before_shutdown():
    print(‘before shutdown’)

App.run(before_shutdown=before_shutdown)
```

In this snippet, `before_shutdown` is called before the app shuts down. That is before the 'while' loop inside App.run comes to an end. This allows you to close connections, tidy up and log your last messages before the app terminates.