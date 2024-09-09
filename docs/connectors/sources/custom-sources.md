# Creating a custom source

Quix streams also provides aset of classes made to help users implement additional sources.

* [`quixstreams.sources.base.BaseSource`]: This is the base class for all other sources. It defines the must have methods.
* [`quixstreams.sources.base.Source`]: A subclass of `BaseSource` implementing a serie of method useful for writing sources. We recommend subclassing `Source` instead of `BaseSource`.

## Source

The recomended parent class to write a new source. It handles configuring, starting, stopping the source as well as implement a serie of helpers.

To get started implement the `run` method and return when `self.running` is `False`.

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

For more information see `quixstreams.sources.base.Source` docstrings.

## BaseSource

This is the base class for all sources. It handles configuring the source and define 3 must have methods.

* `start`: This method is called, in the subprocess, when the source is started.
* `stop`: This method is called, in the subporcess, when the application is shutting down.
* `default_topic`: This method is called, in the main process, when a topic is not provided with the source.

For more information see `quixstreams.sources.base.BaseSource` docstrings.
