# Data serialization

Serialization can be difficult, especially if it’s done with performance in mind. We serialize and deserialize native TimeseriesData transport objects, created specifically to be efficient with time series data. On top of that we use codecs like Protobuf that improve overall performance of the serialization and deserialization process by some orders of magnitude.

![Quix Timeseries Data serialization](../images/QuixStreamsSerialization.png)

Quix Streams automatically serializes data from native types in your language. You can work with familiar types, such as [pandas DataFrame](https://pandas.pydata.org/docs/user_guide/dsintro.html#dataframe), or use our own TimeseriesData without worrying about the conversions that are done for you by the library.
