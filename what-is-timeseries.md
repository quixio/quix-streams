## What is time-series data?

Time-series data is a series of data points indexed in time order. Typically, time-series data is collected at regular intervals, such as days, hours, minutes, seconds, or milliseconds. In a data frame representation, each row of the data frame corresponds to a single time point, and each column contains a different variable or observation measured at that time point.

```
timestamp            value1   value2   value3   value4   value5   value6   value7
2022-01-01 01:20:00  25.3     10.1     32.3     56.2     15.3     12.2     27.1  
2022-01-01 01:20:01  26.2     11.2     31.2     55.1     16.2     13.1     28.2  
2022-01-01 01:20:02  24.1     12.3     30.1     54.3     17.1     14.2     29.1  
2022-01-01 01:20:03  23.4     13.4     29.2     53.2     18.3     15.3     30.2  
2022-01-01 01:20:04  22.6     14.5     28.3     52.1     19.2     16.2     31.1  
2022-01-01 01:20:05  22.4     14.6     28.1     52.8     19.2     16.4     31.1  
...                  ...      ...      ...      ...      ...      ...      ...   
```

Time-series data is often plotted on a graph with the x-axis representing the time at which the data was collected and the y-axis representing the value of the data point.

![Telemetry](./images/telemetry.png)

### High-frequency data

High-frequency data is a type of <b>time-series data</b> that is collected at a very high rate, often at intervals of less than a second. Because high-frequency data is collected at such a high rate, it can be difficult to analyze and visualize using traditional methods. As a result, specialized techniques and tools are often used to process and analyze high-frequency data.

Quix streams is a library specialized in processing <b>high-frequency data</b>, although it can be used to process and analyze any kind of time-series or non-timeseries data.
