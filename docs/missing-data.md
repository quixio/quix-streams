# Missing Data

Handling missing data is a crucial aspect of data processing, especially in streaming environments. This document outlines what is considered missing data within the `StreamingDataFrame` class and how it is managed.

## Types of Missing Data in Streaming

- **Missing Column**: This occurs when a field is not present in the message at all. In streaming data, schemas can be dynamic, meaning that not all fields are required to be present in every message. This type of missing data is handled by the system's ability to adapt to changes in the schema over time.

- **Missing Value**: This occurs when a field is present in the message, but its value is `None`. This indicates that the data for that field is missing, even though the field itself is part of the message schema.

## Handling Missing Data in Aggregations

- **Rows with `None` Values**: These rows are ignored during aggregation operations. This means that if a row contains a `None` value, it will not contribute to the aggregation result. This applies to the following aggregations: Sum, Mean, Min, and Max.

- **`NaN` Values**: Unlike `None`, `NaN` values are propagated to the aggregation result. This is because `NaN` is not considered missing data in the same way as `None`. Instead, it represents a numerical value that is undefined or unrepresentable, and it is treated as such in calculations.

## `StreamingDataFrame.fill` Method

The `fill` method in the `StreamingDataFrame` class is used to fill missing data in the message value with a constant value.

### Example Usage

```python
from quixstreams import Application

# Initialize the Application
app = Application(...)
sdf = app.dataframe(...)
```

Fill missing data for a single column with `None`:
```python
# This would transform {"x": 1} to {"x": 1, "y": None}
sdf.fill("y")
```

Fill missing data for multiple columns with `None`:
```python
# This would transform {"x": 1} to {"x": 1, "y": None, "z": None}
sdf.fill("y", "z")
```

Fill missing data with a constant value using a dictionary:
```python
# This would transform {"x": None} to {"x": 1, "y": 2}
sdf.fill(x=1, y=2)
```

Use a combination of positional and keyword arguments:
```python
# This would transform {"y": None} to {"x": None, "y": 2}
sdf.fill("x", y=2)
```
