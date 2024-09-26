# Creating a custom source

Quix Streams also provides a set of classes to help users implement custom sources.

* [`quixstreams.sources.base.Source`](../../api-reference/sources.md#sources): A subclass of `BaseSource` that implements some helpful methods for writing sources. We recommend subclassing `Source` instead of `BaseSource`.
* [`quixstreams.sources.base.BaseSource`](../../api-reference/sources.md#BaseSource): This is the base class for all other sources. It defines the must have methods.

## Source

The recomended parent class to create a new source. It handles configuring, starting and stopping the source, as well as implementing a series of helpers.

To get started, implement the `run` method and return when `self.running` is `False`.

Example subclass:

```python
from quixstreams.sources.base import Source

class MySource(Source):
    def run(self):
        with open("file.txt", "r") as f:
            while self.running:

                line = f.readline()
                if not line:
                   return

                msg = self.serialize(
                    key="file.txt",
                    value=line.strip(),
                )

                self.produce(
                    key=msg.key,
                    value=msg.value,
                )
```

For more information, see [`quixstreams.sources.base.Source`](../../api-reference/sources.md#source) docstrings.

## BaseSource

This is the base class for all sources. It handles configuring the source and requires the definition of three must-have methods.

* `start`: This method is called, in the subprocess, when the source is started.
* `stop`: This method is called, in the subporcess, when the application is shutting down.
* `default_topic`: This method is called, in the main process, when a topic is not provided with the source.

For more information, see [`quixstreams.sources.base.BaseSource`](../../api-reference/sources.md#BaseSource) docstrings.

## Custom Sources and Jupyter Notebook

Due to the multiprocessing nature of sources, writing a custom one in a Jupyter Notebook doesn't work out of the box.

Running this cell will produce a similar output as below:

```python
from quixstreams import Application
from quixstreams.sources import Source

import random
import time

class MySource(Source):
    def run(self):
        while self.running:
            msg = self.serialize(key="test", value=random.randint(0, 10000))

            self.produce(
                key=msg.key,
                value=msg.value,
            )
            time.sleep(1)

def main():
  app = Application(broker_address="localhost:19092")
  source = MySource(name="mysource")
  
  sdf = app.dataframe(source=source)
  sdf.print(metadata=True)

  app.run()

if __name__ == "__main__":
  main()
```

```
[2024-09-25 10:54:37,852] [INFO] [quixstreams] : Starting the Application with the config: broker_address="{'bootstrap.servers': 'localhost:19092'}" consumer_group="quixstreams-default" auto_offset_reset="latest" commit_interval=5.0s commit_every=0 processing_guarantee="at-least-once"
[2024-09-25 10:54:37,853] [INFO] [quixstreams] : Topics required for this application: "mysource"
[2024-09-25 10:54:37,855] [INFO] [quixstreams] : Creating a new topic "mysource" with config: "{'num_partitions': 1, 'replication_factor': 1, 'extra_config': {}}"
[2024-09-25 10:54:38,856] [INFO] [quixstreams] : Topic "mysource" has been created
[2024-09-25 10:54:38,857] [INFO] [quixstreams] : Validating Kafka topics exist and are configured correctly...
[2024-09-25 10:54:38,859] [INFO] [quixstreams] : Kafka topics validation complete
[2024-09-25 10:54:38,860] [INFO] [quixstreams] : Initializing state directory at "<project path>/state/quixstreams-default"
[2024-09-25 10:54:38,860] [INFO] [quixstreams] : Waiting for incoming messages
[2024-09-25 10:54:39,007] [INFO] [quixstreams] : Starting source mysource
Traceback (most recent call last):
  File "<string>", line 1, in <module>
  File "<env path>/lib/python3.12/multiprocessing/spawn.py", line 122, in spawn_main
    exitcode = _main(fd, parent_sentinel)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "<env path>/lib/python3.12/multiprocessing/spawn.py", line 132, in _main
    self = reduction.pickle.load(from_parent)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AttributeError: Can't get attribute 'MySource' on <module '__main__' (<class '_frozen_importlib.BuiltinImporter'>)>
```

To fix that, you need to define your custom source in a separate file.

```python
%%writefile source.py
# indicate to IPython we want to write this content to a file

from quixstreams.sources import Source

import time
import random

class MySource(Source):
    def run(self):
        while self.running:
            msg = self.serialize(key="test", value=random.randint(0, 10000))

            self.produce(
                key=msg.key,
                value=msg.value,
            )
            time.sleep(1)
```

```python
from quixstreams import Application

from source import MySource

def main():
  app = Application(broker_address="localhost:19092")
  source = MySource(name="mysource")
  
  sdf = app.dataframe(source=source)
  sdf.print(metadata=True)

  app.run()

if __name__ == "__main__":
  main()
```
