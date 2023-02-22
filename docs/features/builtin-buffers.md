# Built-in buffers

If you’re sending data at a high frequency, processing time-series without any buffering involved can be very costly. With smaller messages, data compression cannot be implemented as efficiently. The time spent in serializing and deserializing messages can be improved by sending more values per message.

On the other hand, an incorrect buffer strategy can introduce unnecessary latency which may not be acceptable in some use cases.

Quix Streams provides you with a very high performance, low-latency buffer, combined with easy-to-use configurations to [subscribe](../../subscrice/#buffer) and [publish](../../publish/#buffer) to give you freedom in balancing between latency and cost.

Buffers in the library work at the timestamp level. A buffer accumulates timestamps until a release condition is met. A packet is then published containing those timestamps and values as a [TimeseriesData](../../subscribe/#timeseriesdata-format) package.

![High level time-series buffering flow](../images/QuixBuffering.png)

The logic is simple in theory but gets more complicated when trying to miantain high performance and easy interface. The buffer can be used to [subscribe](../../subscribe/#buffer) to and [publish](../../publish/#buffer) time-series data.

Our buffer implementation uses short memory allocations and minimizes conversions between raw transport packages and [TimeseriesData](../../consume/#timeseriesdata-format) format to achieve low CPU and memory consumption with high throughput. We are happy to claim that our implementation has all of these, — simplicity, low resource consumption, and high performance —, therefore you don’t need to implement buffering because it is provided in the library.
