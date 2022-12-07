# Data serialization

Serialization can be painful, especially if it’s done with performance
in mind. We serialize and deserialize native ParameterData transport
objects, created specifically to be efficient with time series data. On
top of that we use codecs like Protobuf that improve overall performance
of the serialization and deserialization process by some orders of
magnitude.

![Quix Parameter Data serialization](../images/QuixSdkSerialization.png)

The Quix SDK automatically serializes data from native types in your
language. You can work with familiar types, such as [Pandas
DataFrames](https://pandas.pydata.org/docs/user_guide/dsintro.html#dataframe),
or use our own ParameterData class without worrying about the
conversions that happen behind the scenes.
