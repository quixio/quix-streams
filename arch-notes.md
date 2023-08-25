## Library architecture notes

### Interoperability wrappers

Quix Streams base library is developed in C#. We use Interoperability wrappers around <b>C# AoT (Ahead of Time) compiled code</b> to implement support for other languages such as <b>Python</b>. These Interop wrappers are auto-generated using a project called `InteropGenerator` included in the same repository. Ahead-of-time native compilation was a feature introduced officially on .NET 7. Learn more [here](https://learn.microsoft.com/en-us/dotnet/core/deploying/native-aot/).

You can generate these Wrappers again using the `shell scripts` provided for each platform inside the language-specific client. For instance for Python:

- `/src/builds/python/windows/build_native.bat`: Generates Python Interop wrappers for Windows platform.
- `/src/builds/python/linux/build_native.bat`: Generates Python Interop wrappers for Linux platform.
- `/src/builds/python/mac/build_native.bat`: Generates Python Interop wrappers for Mac platform.

These scripts compile the C# base library and then use the `InteropGenerator` project to generate the AoT compiled version of the library and the Interop wrappers around that. The result is a structure like this:

```

   ┌───────────────────────────┐
   │   Python client library   │    /Python/lib/quixstreams
   └─────────────┬─────────────┘
                 │
                 │
   ┌─────────────▼─────────────┐
   │  Python Interop wrapper   │    /Python/lib/quixstreams/native/Python  (auto-generated)
   └─────────────┬─────────────┘
                 │
                 │
   ┌─────────────▼─────────────┐
   │  C# AoT compiled library  │    /Python/lib/quixstreams/native/win64   (auto-generated)
   └───────────────────────────┘

```

The non auto-generated `Python client library` still needs to be maintained manually, but this is expected because each language has its own language-specific features and naming conventions that we want to keep aligned with the language user expectations. If you want to add a new feature of the library that is common to all the languages, you should implement that feature in the C# base library first, re-generate the Interop wrappers, and then modify the Python client library to wire up the new feature of the base library.

### Base library

Quix Streams base library is implemented in C#, therefore if your target language is C#, you will use that base library without any [Interoperability wrapper](#interoperability-wrappers) involved on the execution. 

This base library is organized in 3 main layers:

```

   ┌───────────────────────────┐
   │      Streaming layer      │    /CSharp/QuixStreams.Streaming
   └─────────────┬─────────────┘
                 │
                 │
   ┌─────────────▼─────────────┐
   │      Telemetry layer      │    /CSharp/QuixStreams.Telemetry
   └─────────────┬─────────────┘
                 │
                 │
   ┌─────────────▼─────────────┐
   │   Kafka Transport layer   │    /CSharp/QuixStreams.Kafka.Transport
   ├───────────────────────────┤
   │    Kafka wrapper layer    │    /CSharp/QuixStreams.Kafka
   └───────────────────────────┘

```

Each layer has his own responsibilities:
 
- <b>Streaming layer</b>: This is the user facing layer of the library. It includes all the <b>syntax sugar</b> needed to have a pleasant experience with the library.

- <b>Telemetry layer</b>: This layer implements the `Codecs` to ser/des the <b>Telemetry messages</b> of Quix Streams protocol. This includes time-series and non time-series messages, stream metadata, stream properties messages, parameters definitions, as well as creating the [Stream context](#library-features-) scopes. The layer also implements a `Stream Pipeline` system to concatenate different components that can be used to implement complex low-level Telemetry services.

- <b>Transport layer</b>: This layer is responsible for handling <b>communication with the kafka broker</b>. It introduces additional features to ease the use of Kafka, improves on some of the broker's limitations and contains workarounds to some known issues. The library relies on [Confluent .NET Client for Apache Kafka](https://github.com/confluentinc/confluent-kafka-dotnet).

For more information and general questions about the architecture of the library you can join to our official [Slack channel](https://quix.io/slack-invite).