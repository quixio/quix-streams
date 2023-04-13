#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Configuration](QuixStreams.Streaming.Configuration.md 'QuixStreams.Streaming.Configuration')

## SecurityOptions Class

A class representing security options for configuring SSL encryption with SASL authentication in Kafka.

```csharp
public class SecurityOptions
```

Inheritance [System.Object](https://docs.microsoft.com/en-us/dotnet/api/System.Object 'System.Object') &#129106; SecurityOptions

| Constructors | |
| :--- | :--- |
| [SecurityOptions()](SecurityOptions.SecurityOptions().md 'QuixStreams.Streaming.Configuration.SecurityOptions.SecurityOptions()') | For deserialization when binding to Configurations like Appsettings |
| [SecurityOptions(string, string, string, SaslMechanism)](SecurityOptions.SecurityOptions(string,string,string,SaslMechanism).md 'QuixStreams.Streaming.Configuration.SecurityOptions.SecurityOptions(string, string, string, QuixStreams.Streaming.Configuration.SaslMechanism)') | Initializes a new instance of [SecurityOptions](SecurityOptions.md 'QuixStreams.Streaming.Configuration.SecurityOptions') that is configured for SSL encryption with SASL authentication |

| Properties | |
| :--- | :--- |
| [Password](SecurityOptions.Password.md 'QuixStreams.Streaming.Configuration.SecurityOptions.Password') | The password for SASL authentication |
| [SaslMechanism](SecurityOptions.SaslMechanism.md 'QuixStreams.Streaming.Configuration.SecurityOptions.SaslMechanism') | The SASL mechanism to use. |
| [SslCertificates](SecurityOptions.SslCertificates.md 'QuixStreams.Streaming.Configuration.SecurityOptions.SslCertificates') | The path to the folder or file containing the certificate authority certificate(s) to validate the ssl connection. |
| [UseSasl](SecurityOptions.UseSasl.md 'QuixStreams.Streaming.Configuration.SecurityOptions.UseSasl') | Use authentication |
| [UseSsl](SecurityOptions.UseSsl.md 'QuixStreams.Streaming.Configuration.SecurityOptions.UseSsl') | Use SSL |
| [Username](SecurityOptions.Username.md 'QuixStreams.Streaming.Configuration.SecurityOptions.Username') | The username for SASL authentication. |
