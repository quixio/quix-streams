# CSV Source

A basic source that reads data from a single CSV file.

The CSV Source doesn't keep any state. On restart, the file will be re-consumed from the start.

## How to use CSV Source

To use a CSV Source, you need to create and instance of `CSVSource` and pass it to the `app.dataframe()` method.

```python
from quixstreams import Application
from quixstreams.sources import CSVSource

def main():
  app = Application()
  source = CSVSource(path="input.csv")

  sdf = app.dataframe(source=source)
  sdf.print(metadata=True)

  app.run(sdf)

if __name__ == "__main__":
  main()
```

## File format

The CSV source expect the input file to have headers, a `key` column, a `value` column and optionally a `timestamp` column.

Example file:

```csv
key,value
foo1,bar1
foo2,bar2
foo3,bar3
foo4,bar4
foo5,bar5
foo6,bar6
foo7,bar7
```

## Key and value format

By default the CSV source expect the `key` is a string and the `value` a json object. You can configure the deserializers using the `key_deserializer` and `value_deserializer` paramaters.

## Topic

The default topic used for the CSV source will use the `path` as a name and expect keys to be strings and values to be JSON objects.  
