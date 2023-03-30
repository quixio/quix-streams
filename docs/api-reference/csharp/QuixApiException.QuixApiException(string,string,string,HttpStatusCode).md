#### [QuixStreams.Streaming](index.md 'index')
### [QuixStreams.Streaming.QuixApi](QuixStreams.Streaming.QuixApi.md 'QuixStreams.Streaming.QuixApi').[QuixApiException](QuixApiException.md 'QuixStreams.Streaming.QuixApi.QuixApiException')

## QuixApiException(string, string, string, HttpStatusCode) Constructor

Initializes a new instance of [QuixApiException](QuixApiException.md 'QuixStreams.Streaming.QuixApi.QuixApiException')

```csharp
public QuixApiException(string endpoint, string msg, string cid, System.Net.HttpStatusCode httpStatusCode);
```
#### Parameters

<a name='QuixStreams.Streaming.QuixApi.QuixApiException.QuixApiException(string,string,string,System.Net.HttpStatusCode).endpoint'></a>

`endpoint` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Endpoint used to access Quix Api

<a name='QuixStreams.Streaming.QuixApi.QuixApiException.QuixApiException(string,string,string,System.Net.HttpStatusCode).msg'></a>

`msg` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Error message

<a name='QuixStreams.Streaming.QuixApi.QuixApiException.QuixApiException(string,string,string,System.Net.HttpStatusCode).cid'></a>

`cid` [System.String](https://docs.microsoft.com/en-us/dotnet/api/System.String 'System.String')

Correlation Id

<a name='QuixStreams.Streaming.QuixApi.QuixApiException.QuixApiException(string,string,string,System.Net.HttpStatusCode).httpStatusCode'></a>

`httpStatusCode` [System.Net.HttpStatusCode](https://docs.microsoft.com/en-us/dotnet/api/System.Net.HttpStatusCode 'System.Net.HttpStatusCode')

Http error code