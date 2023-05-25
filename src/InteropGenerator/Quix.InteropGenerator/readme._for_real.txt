Notes:
Interop best practices: https://docs.microsoft.com/en-us/dotnet/standard/native-interop/best-practices

When running the application in IDE, use the following program arguments:
-a "../../../../../CsharpClient/QuixStreams.Streaming/bin/Debug/net8.0/publish/QuixStreams.Streaming.dll" -o "../../../../InteropOutput"  -c "../../../../InteropConfig"

note: You'll need to publish QuixStreams.Streaming first to have all dependency available. If you're publishing other than Debug config, update line above accordingly


Additional sources:
https://github.com/dotnet/corert/issues/5496 (to deal with missing clrcompression.dll)
https://coderator.net/c-experimental-serisi-native-kutuphane-derliyoruz/ (helpful to get the setup done)
https://github.com/dotnet/corert/issues/7096 (solved missing ubuntu lib