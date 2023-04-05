#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.Configuration](QuixStreams.Streaming.Configuration.md 'QuixStreams.Streaming.Configuration').[SecurityOptions](SecurityOptions.md 'QuixStreams.Streaming.Configuration.SecurityOptions')

## SecurityOptions(string, string, string, SaslMechanism) Constructor

Initializes a new instance of [SecurityOptions](SecurityOptions.md 'QuixStreams.Streaming.Configuration.SecurityOptions') that is configured for SSL encryption with SASL authentication

```csharp
public SecurityOptions(string sslCertificates, string username, string password, QuixStreams.Streaming.Configuration.SaslMechanism saslMechanism=QuixStreams.Streaming.Configuration.SaslMechanism.ScramSha256);
```
#### Parameters

<a name='QuixStreams.Streaming.Configuration.SecurityOptions.SecurityOptions(string,string,string,QuixStreams.Streaming.Configuration.SaslMechanism).sslCertificates'></a>

`sslCertificates` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

The path to the folder or file containing the certificate authority certificate(s) to validate the ssl connection. Example: "./certificates/ca.cert"

<a name='QuixStreams.Streaming.Configuration.SecurityOptions.SecurityOptions(string,string,string,QuixStreams.Streaming.Configuration.SaslMechanism).username'></a>

`username` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

The username for the SASL authentication

<a name='QuixStreams.Streaming.Configuration.SecurityOptions.SecurityOptions(string,string,string,QuixStreams.Streaming.Configuration.SaslMechanism).password'></a>

`password` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

The password for the SASL authentication

<a name='QuixStreams.Streaming.Configuration.SecurityOptions.SecurityOptions(string,string,string,QuixStreams.Streaming.Configuration.SaslMechanism).saslMechanism'></a>

`saslMechanism` [SaslMechanism](SaslMechanism.md 'QuixStreams.Streaming.Configuration.SaslMechanism')

The SASL mechanism to use. Defaulting to ScramSha256