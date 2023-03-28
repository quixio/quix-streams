#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[QuixStreamingClient](QuixStreamingClient.md 'QuixStreams.Streaming.QuixStreamingClient').[TokenValidationConfiguration](QuixStreamingClient.TokenValidationConfiguration.md 'QuixStreams.Streaming.QuixStreamingClient.TokenValidationConfiguration')

## QuixStreamingClient.TokenValidationConfiguration.WarningBeforeExpiry Field

If the token expires within this period, a warning will be displayed. Defaults to `2 days`. Set to null to disable the check

```csharp
public Nullable<TimeSpan> WarningBeforeExpiry;
```

#### Field Value
[System.Nullable&lt;](https://docs.microsoft.com/en-us/dotnet/api/System.Nullable-1 'System.Nullable`1')[System.TimeSpan](https://docs.microsoft.com/en-us/dotnet/api/System.TimeSpan 'System.TimeSpan')[&gt;](https://docs.microsoft.com/en-us/dotnet/api/System.Nullable-1 'System.Nullable`1')