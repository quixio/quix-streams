# `StreamingDataFrame.concat()`: concatenating multiple topics into a stream 

Use `StreamingDataFrame.concat()` to combine two or more topics into a new stream containing all the elements from all the topics.

Use it when you need:

- To process multiple topics as a single stream.
- To combine the branches of the same StreamingDataFrame back together.

## Examples

**Example 1:**  Aggregate e-commerce orders from different locations into one stream and calculate the average order size in 1h windows.

```python
from datetime import timedelta

from quixstreams import Application
from quixstreams.dataframe.windows import Mean

app = Application(...)

# Define the orders topics 
topic_uk = app.topic("orders-uk")
topic_de = app.topic("orders-de")

# Create StreamingDataFrames for each location
orders_uk = app.dataframe(topic_uk)
orders_de = app.dataframe(topic_de)

# Simulate the currency conversion step for each topic before concatenating them.
orders_uk["amount_usd"] = orders_uk["amount"].apply(convert_currency("GBP", "USD"))

orders_de["amount_usd"] = orders_de["amount"].apply(convert_currency("EUR", "USD"))

# Concatenate the orders from different locations into a new StreamingDataFrame.
# The new dataframe will have all records from both topics.
orders_combined = orders_uk.concat(orders_de)

# Calculate the average order size in USD within 1h tumbling window. 
orders_combined.tumbling_window(timedelta(hours=1)).agg(avg_amount_usd=Mean("amount_usd"))


if __name__ == '__main__':
    app.run()
```


**Example 2:** Combine branches of the same `StreamingDataFrame` back together.  
See the [Branching](branching.md) page for more details about branching.

```python
from quixstreams import Application
app = Application(...)

input_topic = app.topic("orders")
output_topic = app.topic("output")

# Create a dataframe with all orders
all_orders = app.dataframe(input_topic)

# Create a branches with DE and UK orders:
orders_de = all_orders[all_orders["country"] == "DE"]
orders_uk = all_orders[all_orders["country"] == "UK"]

# Do some conditional processing for DE and UK orders here
# ...

# Combine the branches back with .concat()
all_orders = orders_de.concat(orders_uk)

# Send data to the output topic
all_orders.to_topic(output_topic)


if __name__ == '__main__':
    app.run()
 ```


## Message ordering between partitions
When using `StreamingDataFrame.concat()` to combine different topics, the application's internal consumer goes into a special "buffered" mode.  

In this mode, it buffers messages per partition in order to process them in the timestamp order between different topics.  
Timestamp alignment is effective only for the partitions **with the same numbers**: partition zero is aligned with other zero partitions, but not with partition one. 

Why is this needed?  
Consider two topics A and B with the following timestamps:

- **Topic A (partition 0):** 11, 15
- **Topic B (partition 0):** 12, 17

By default, Kafka does not guarantee the processing order to be **11**, **12**, **15**, **17** because the order is guaranteed only within a single partition.

With timestamp alignment, the order is achievable given that the messages are already present in the topic partitions (i.e. it doesn't handle the cases when the producer is delayed).


## Stateful operations on concatenated dataframes

To perform stateful operations like windowed aggregations on the concatenated StreamingDataFrame, the underlying topics **must have the same number of partitions**.  
The application will raise the error when this condition is not met.

In addition, **the message keys must be distributed using the same partitioning algorithm.**  
Otherwise, same keys may access different state stores leading to incorrect results.
