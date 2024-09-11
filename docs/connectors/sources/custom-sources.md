# Creating a custom source

Quix Streams also provides a set of classes to help users implement custom sources.

* [`quixstreams.sources.base.Source`](../../api-reference/sources.md#sources): A subclass of `BaseSource` that implements some helpful methods for writing sources. We recommend subclassing `Source` instead of `BaseSource`.
* [`quixstreams.sources.base.BaseSource`](../../api-reference/sources.md#BaseSource): This is the base class for all other sources. It defines the must have methods.

## Source

The recomended parent class to create a new source. It handles configuring, starting and stopping the source, as well as implementing a serie of helpers.

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
