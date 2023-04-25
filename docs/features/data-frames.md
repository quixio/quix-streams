# Support for Data Frames

Quix Streams supports the [pandas DataFrame](https://pandas.pydata.org/docs/user_guide/dsintro.html#dataframe) format.

When you [subscribe](../subscribe.md#pandas-dataframe-format) to a topic or [publish](../publish.md#pandas-dataframe-format) data to a topic, you can choose to use this format. 

The client library uses the common [`TimeseriesData`](../subscribe.md#timeseriesdata-format) format internally, but handles the conversion to pandas data frame format for you.

For example, the following [`TimeseriesData`](../subscribe.md#timeseriesdata-format) data:

| Timestamp | CarId (tag) | Speed | Gear |
| --------- | ----------- | ----- | ---- |
| 1         | car-1       | 120   | 3    |
| 1         | car-2       | 123   | 3    |
| 3         | car-1       | 125   | 3    |
| 6         | car-2       | 110   | 2    |

Is represented as the following pandas DataFrame:

| time | TAG\_\_CarId | Speed | Gear |
| ---- | ------------ | ----- | ---- |
| 1    | car-1        | 120   | 3    |
| 1    | car-2        | 123   | 3    |
| 3    | car-1        | 125   | 3    |
| 6    | car-2        | 110   | 2    |

Quix Streams provides multiple methods and events that work directly with [pandas DataFrame](https://pandas.pydata.org/docs/user_guide/dsintro.html#dataframe).

## Next steps

Please refer to the sections [Using Data Frames for subscribing](../subscribe.md#pandas-dataframe-format) and [Using Data Frames for publishing](../publish.md#pandas-dataframe-format) for further information.
