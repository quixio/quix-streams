# Support for Data Frames

Quix Streams supports [pandas DataFrame](https://pandas.pydata.org/docs/user_guide/dsintro.html#dataframe) for [subscribing](../subscribe#using_data_frames) and [publishing](../produce#using-data-frames) to topics. The library still uses the common [TimeseriesData](../consume.md#timeseriesdata-format) internally, but handles the conversion seamlessly for you.

For example, the following [TimeseriesData](../consume.md#timeseriesdata-format):

| Timestamp | CarId (tag) | Speed | Gear |
| --------- | ----------- | ----- | ---- |
| 1         | car-1       | 120   | 3    |
| 1         | car-2       | 123   | 3    |
| 3         | car-1       | 125   | 3    |
| 6         | car-2       | 110   | 2    |

Is represented as the following pandas Data Frame:

| time | TAG\_\_CarId | Speed | Gear |
| ---- | ------------ | ----- | ---- |
| 1    | car-1        | 120   | 3    |
| 1    | car-2        | 123   | 3    |
| 3    | car-1        | 125   | 3    |
| 6    | car-2        | 110   | 2    |

Quix Streams provides multiple methods and events that work directly with [pandas DataFrame](https://pandas.pydata.org/docs/user_guide/dsintro.html#dataframe).

Please refer to the sections [Using Data Frames for subscribing](../subscribe.md#pandas-dataframe-format) and [Using Data Frames for publishing](../publish.md#pandas-dataframe-format) for extended information.
