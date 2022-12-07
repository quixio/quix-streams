# Support for Data Frames

The Quix SDK supports [reading](#../read.adoc#_using_data_frames) and
[writing](#../write.adoc#_using_data_frames) data using [Pandas
DataFrames](https://pandas.pydata.org/docs/user_guide/dsintro.html#dataframe).
If you use the Python version of the SDK you can make use of this
library together with Quix, ensuring maximum optimization for your
real-time applications.

The SDK uses Pandas DataFrames just as a representation of the common
[ParameterData](#parameter-data-format) format used to read and write
data to Quix.

For example, the following [ParameterData](#parameter-data-format):

| Timestamp | CarId (tag) | Speed | Gear |
| --------- | ----------- | ----- | ---- |
| 1         | car-1       | 120   | 3    |
| 2         | car-2       | 123   | 3    |
| 3         | car-1       | 125   | 3    |
| 6         | car-2       | 110   | 2    |

An example of ParameterData

Is represented as the following Pandas Data Frame:

| time | TAG\_\_CarId | Speed | Gear |
| ---- | ------------ | ----- | ---- |
| 1    | car-1        | 120   | 3    |
| 2    | car-2        | 123   | 3    |
| 3    | car-1        | 125   | 3    |
| 6    | car-2        | 110   | 2    |

A representation of ParameterData in a Pandas Data Frame

The Quix SDK provides multiple methods and events that work directly
with [Pandas
DataFrames](https://pandas.pydata.org/docs/user_guide/dsintro.html#dataframe).

Please refer to the sections [Using Data Frames for reading from
Quix](#../read.adoc#_using_data_frames) and [Using Data Frames for
writing to Quix](#../read.adoc#_using_data_frames) for extended
information.
