# Data serialization

Serialization can be difficult, especially if it’s done with performance in mind. Quix Streams serializes and deserializes `TimeseriesData` transport objects, which was created specifically to be efficient with time series data. Codecs such as Protobuf are also used to significantly improve overall performance of the serialization and deserialization process.

![Quix Timeseries Data serialization](../images/QuixStreamsSerialization.png)

Quix Streams automatically serializes data from your language's built in types. You can work with familiar types, such as [pandas DataFrame](https://pandas.pydata.org/docs/user_guide/dsintro.html#dataframe), or use the Quix `TimeseriesData` type, and leave the library to perform any type conversions required.
