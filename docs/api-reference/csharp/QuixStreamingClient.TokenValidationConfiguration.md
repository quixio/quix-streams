#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming](QuixStreams.Streaming.md 'QuixStreams.Streaming').[QuixStreamingClient](QuixStreamingClient.md 'QuixStreams.Streaming.QuixStreamingClient')

## QuixStreamingClient.TokenValidationConfiguration Class

Token Validation configuration

```csharp
public class QuixStreamingClient.TokenValidationConfiguration
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; TokenValidationConfiguration

| Fields | |
| :--- | :--- |
| [Enabled](QuixStreamingClient.TokenValidationConfiguration.Enabled.md 'QuixStreams.Streaming.QuixStreamingClient.TokenValidationConfiguration.Enabled') | Whether token validation and warnings are enabled. Defaults to `true`. |
| [WarnAboutNonPatToken](QuixStreamingClient.TokenValidationConfiguration.WarnAboutNonPatToken.md 'QuixStreams.Streaming.QuixStreamingClient.TokenValidationConfiguration.WarnAboutNonPatToken') | Whether to warn if the provided token is not PAT token. Defaults to `true`. |
| [WarningBeforeExpiry](QuixStreamingClient.TokenValidationConfiguration.WarningBeforeExpiry.md 'QuixStreams.Streaming.QuixStreamingClient.TokenValidationConfiguration.WarningBeforeExpiry') | If the token expires within this period, a warning will be displayed. Defaults to `2 days`. Set to null to disable the check |
