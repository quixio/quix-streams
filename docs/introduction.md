# Quix Streams client library

Quix Streams v2 is a cloud native library for processing data in Kafka using pure Python. It’s designed to give you the power of a distributed system in a lightweight library by combining the low-level scalability and resiliency features of Kafka with an easy to use Python interface.

Quix Streams has the following benefits:

* No JVM, no orchestrator, no server-side engine.
* Easily integrates with the entire Python ecosystem (pandas, scikit-learn, TensorFlow, PyTorch etc).
* Support for many serialization formats, including JSON (and Quix-specific).
* Support for stateful operations using RocksDB.
* Support for aggregations over tumbling and hopping time windows
* A simple framework with Pandas-like interface to ease newcomers to streaming.
* "At-least-once" Kafka processing guarantees.
* Designed to run and scale resiliently via container orchestration (like Kubernetes).
* Easily runs locally and in Jupyter Notebook for convenient development and debugging.
* Seamless integration with the Quix platform.
* Use Quix Streams to build event-driven, machine learning/AI or physics-based applications that depend on real-time data from Kafka.

See the [Quix Streams GitHub page](https://github.com/quixio/quix-streams) for detailed project information, and all source code.

## Next steps

- [Quix Streams Quickstart](quickstart.md)

Check out Quix Streams tutorials for more in-depth examples:

- [Tutorial - Word Count](tutorials/word-count/tutorial.md)
- [Tutorial - Anomaly Detection](tutorials/anomaly-detection/tutorial.md)
- [Tutorial - Purchase Filtering](tutorials/purchase-filtering/tutorial.md) 
- [Tutorial - Websocket Source](tutorials/websocket-source/tutorial.md) 
