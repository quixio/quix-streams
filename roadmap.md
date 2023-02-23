# Roadmap
In this page you will find features considered for development or currently being developed.

## Streaming Data frames
Stateful stream processing is difficult and requires a very different mindset compared to batch. The goal of streaming data frames is to bridge the gap between streaming and batch and make it easier for users coming from the batch. 

Let's illustrate the problem on a very standard example of stateful processing - a rolling window of the last 10 minutes of data. If you perform this operation by yourself, you need to keep an eye on the following:

- Management of state in memory
- Keep the current rolling window in memory and remove old rows outside the window
    - Append new rows and produce new output rows
    - Kafka Checkpointing working hand to hand with state persistence to database
- Internal queue with incoming messages consumed by workers is synchronized with checkpointing
- State recovery after service restart

What we are working on is solving all these common problems by adding some new features to the library, giving it a familiar interface of Pandas DataFrames.

This is an example code where a rolling window is performed in a streaming data frame but the code looks exactly how you would do this in Jupyter notebook on static data:
```python
# Create a projection for columns we need.
df = input_stream.df[["gForceX", "gForceY", "gForceZ"]] 

# Create new feature by simply combining three columns to one new column.
df["gForceTotal"] = df["gForceX"].abs() + df["gForceY"].abs() + df["gForceZ"].abs()

# Calculate rolling window of previous column for last 10 minutes
df["gForceTotal_avg10s"] = df["gForceTotal"].rolling("10m").mean()

# Loop through the stream row by row as data frow through the service. 
# Async iterator will stop the code if there is no new data incoming from i 
async for row in df:
    print(row)
    await output_stream.write(row)
```

### Benefits
- People don't need to train new API, batch skills set is enough to get going.
- The complexity of stateful processing is done under the hood so no need to understand it first to start.
- Because under the hood is not actually Pandas but binary tables, a significant performance boost is delivered compared to using traditional pandas data frames.



