# Built-in buffers

If you’re sending data at a high frequency, processing time-series data without any buffering involved can be very costly. With smaller messages, data compression cannot be implemented as efficiently. The time spent in serializing and deserializing messages can be improved by sending more values per message.

On the other hand, an incorrect buffer strategy can introduce unnecessary latency which may not be acceptable in some use cases.

Quix Streams provides you with a high performance and low-latency buffer, and a simple way to  configure buffer parameters. This enables you to configure the buffer to balance latency and cost of memeory allocated to buffers.

Buffers in the library work at the timestamp level. A buffer accumulates timestamps until a release condition is met. A packet is then published containing those timestamps and values as a [TimeseriesData](../subscribe.md#timeseriesdata-format) package.

![High level time-series buffering flow](../images/QuixBuffering.png)

The buffer can be used to [subscribe](../subscribe.md#using-a-buffer) to and [publish](../publish.md#using-a-buffer) time-series data.

The Quix Streams buffer implementation uses short-lived memory allocations and minimizes conversions between raw transport packages and the [TimeseriesData](../subscribe.md#timeseriesdata-format) format, to achieve low CPU and memory consumption, while retaining high throughput. 
