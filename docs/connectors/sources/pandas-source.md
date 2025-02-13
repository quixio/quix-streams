# Pandas DataFrame Source

A source that reads data from a Pandas DataFrame file and produces rows to the Kafka topic in JSON format.

The PandasDataFrameSource reads the dataframe, produces the data and exit.  
It doesn't keep any state.  
On restart, the whole file will be re-consumed.


## How To Install

Install Quix Streams with the following optional dependencies:

```bash
pip install quixstreams[pandas]
```

## How To Use

To use it, you need to create and instance of `PandasDataFrameSource` and pass it to the `app.dataframe()` method.

```python
import pandas as pd
from quixstreams import Application
from quixstreams.sources.community.pandas import PandasDataFrameSource


def main():
    app = Application()
    
    # Read a DataFrame from a file.
    # File format: "Timestamp", "SessionID", "Metric"
    df = pd.read_csv("data.csv")
    
    
    # Create the PandasDataFrameSource instance and pass the df to it
    source = PandasDataFrameSource(
        df=df, # DataFrame to read data from 
        key_column="SessionID",  # A column to be used for message keys
        timestamp_column="Timestamp", # A column to be used for message timestamps
        name="data_csv-dataframe" # The name will be included to the default topic name.
    )
        
    # Pass the source instance to a StreamingDataFrame and start the application
    sdf = app.dataframe(source=source)
    sdf.print(metadata=True)

    app.run()


if __name__ == "__main__":
    main()
```

## Simulating real-time streaming with `delay`
To simulate data coming in real-time, you can pass the `delay` param to the source class.

If passed, the source will sleep for the specified number of seconds between each row.


## Key and timestamp columns
To specify a column to be used for message keys, pass its name as `key_column` to `PandasDataFrameSource`.
The values must be either strings or `None`. 

To specify a column for message timestamps, pass its name as `timestamp_column`.
Timestamp values must be integers and they are expected to represent milliseconds.

`timestamp_column` is optional.  
If not passed, the current time will be used


## Topic

The default topic used for the Pandas DataFrame source will use the `name` as a part of the topic name and expect keys to be strings and values to be JSON objects.  
