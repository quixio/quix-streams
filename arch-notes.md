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
   │      Streaming layer      │    /CSharp/Quix.Streams.Streaming
   └─────────────┬─────────────┘
                 │
                 │
   ┌─────────────▼─────────────┐
   │      Telemetry layer      │    /CSharp/Quix.Streams.Telemetry
   └─────────────┬─────────────┘
                 │
                 │
   ┌─────────────▼─────────────┐
   │   Kafka Transport layer   │    /CSharp/Quix.Streams.Transport.Kafka
   ├───────────────────────────┤
   │      Transport layer      │    /CSharp/Quix.Streams.Transport
   └───────────────────────────┘

```

Each layer has his own responsibilities:
 
- <b>Streaming layer</b>: This is the main layer of the library that users should use by default. It includes all the <b>syntax sugar</b> needed to have a pleasant experience with the library. Another important responsibility of this layer is the <b>embedded time-series buffer</b> system.

- <b>Telemetry layer</b>: This layer is responsible for implementing the `Codecs` serialization and de-serialization for all the <b>Telemetry messages</b> of Quix Streams protocol. This includes time-series and non time-series messages, stream metadata, stream properties messages, parameters definitions, as well as creating the [Stream context](#library-features-) scopes responsible for the separation between data coming from different sources. This layer also implements a `Stream Pipeline` system to concatenate different Stream processes that can be used to implement complex low-level Telemetry services.

- <b>Transport layer</b>: This layer is responsible for the <b>communication with the message broker</b> and implementing some features to deal with the message broker's features and limitations. Some of these features are `message splitting`, `checkpointing`, `partition revocation`, `connectivity issues recovering` among others. This layer is also responsible for implementing a `wrapping messages system` to allow different message types of the library Protocol, and to define the base classes for the `Codecs` implementation of each messages of that Protocol on the upper layers of the library. For <b>Kafka</b> support, this base library uses internally [Confluent .NET Client for Apache Kafka](https://github.com/confluentinc/confluent-kafka-dotnet), which uses the library [librdkafka - the Apache Kafka C/C++ client library](https://github.com/edenhill/librdkafka).

For more information and general questions about the architecture of the library you can join to our official [Slack channel](https://quix.io/slack-invite).