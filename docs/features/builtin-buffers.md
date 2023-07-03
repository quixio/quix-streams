# Built-in buffers
  
Quix Streams offers high-performance, low-latency buffering capabilities, providing an optimized method for sending and processing time-series data. The buffering mechanism used here helps reduce costs, mainly when dealing with high-frequency data, by improving data compression and reducing serialization and deserialization time.

Buffers in Quix Streams work at the timestamp level, accumulating timestamps until a specific release condition is met. A packet is then published containing those timestamps and values as a [TimeseriesData](../subscribe.md#timeseriesdata-format) package.

 
![High level time-series buffering flow](../images/QuixBuffering.png)


The buffer can be used to [subscribe](../subscribe.md#using-a-buffer) to and [publish](../publish.md#using-a-buffer) time-series data.

 ## Leading edge buffer
 In addition to the package size, timespan and timeout buffer configurations, Quix Streams also offers the `LeadingEdgeBuffer` – a buffer that can be especially useful when data received is out-of-order and must be ordered before it is consumed and late arriving data.

```csharp
// Create LeadingEdgeBuffer with leading edge delay of 1000ms
var leadingEdgeBuffer = stream.Timeseries.Buffer.CreateLeadingEdgeBuffer(1000);

buffer.OnBackfill += (sender, row) => { /* handle backfill */ };
buffer.OnPublish += (sender, data) => { /* handle publish */ };

foreach (var data in unorderedData)
{
	var timestamp = buffer.GetOrCreateTimestamp(data.Timestamp);
	timestamp.AddValue(parameterId, data.Value, overwrite: data.ShouldOverwrite)
}

buffer.Publish();
```

Note: The example code provided is in C#, and as of now, the Python implementation does not exist yet

    
The leading edge delay configuration gives out-of-order data a chance to be sorted in the correct order.
Setting a leading edge delay means that you're not processing the data in real-time. Instead, you're processing it near real-time, with a delay equal to the leading edge delay you've set.
This trade-off is often worth it, as it reduces the likelihood of misinterpreting your data due to missing out on late-arriving data points.    
    

Buffers are designed to handle streaming data that arrives in real-time, and they usually expect this data to arrive in order.
However, there can be instances when data doesn't arrive in the expected order due to issues like network delays or processing times at the source.
The `OnBackfill` event is triggered to handle these instances. `OnBackfill` event is triggered when a data point arrives that has a timestamp older than the most recent or "last released" timestamp from the buffer.