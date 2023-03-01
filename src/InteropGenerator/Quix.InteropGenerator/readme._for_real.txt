Notes:
- Version of the LLVM library is super important. Managed to get it to work with 6.0.0-alpha.1.20606.1, but not with other versions yet

- You must build it in debug mode, because release mode comes up with the following error:
```
EXEC : error : VTable of type 'Confluent.Kafka.ProduceException`2<Byte[], Byte[]>' not computed by the IL scanner. You can work around by running the compilation with scanner disabled. [C:\Work\Source\QuixStreams\CsharpClient\QuixStreams.Interop\QuixStreams.Interop.csproj]
```

- From a clean code, execute the following to get win built:
```
dotnet publish /p:NativeLib=Static -r win-x64 -c debug
dotnet publish /p:NativeLib=Shared -r win-x64 -c debug
```
Note:
You need to install c++ compiler MSVC tool in addition to dotnet5

- From a clean code, execute the following to get linux built:
```
dotnet publish /p:NativeLib=Static -r linux-x64 -c debug
dotnet publish /p:NativeLib=Shared -r linux-x64 -c debug
```
NOTE: You must build inside a linux machine. The LLVM nuget we use is not cross OS ready yet.
Necessary things to install in ubuntu:
```
sudo apt-get install -y cmake llvm-9 clang-9 autoconf automake \
libtool build-essential python curl git lldb-6.0 liblldb-6.0-dev \
libunwind8 libunwind8-dev gettext libicu-dev liblttng-ust-dev \
libssl-dev libnuma-dev libkrb5-dev zlib1g-dev ninja-build libtinfo5 
```
- I temporarily had to remove memory cache to avoid runtime issues... This will need to be added back

Interop best practices: https://docs.microsoft.com/en-us/dotnet/standard/native-interop/best-practices

When running the application in IDE, use the following program arguments:
-a "../../../../../CsharpClient/QuixStreams.Streaming/bin/Debug/netstandard2.0/publish/QuixStreams.Streaming.dll" -o "../../../../InteropOutput"  -c "../../../../InteropConfig"

note: You'll need to publish QuixStreams.Streaming first to have all dependency available. If you're publishing other than Debug config, update line above accordingly


Additional sources:
https://github.com/dotnet/corert/issues/5496 (to deal with missing clrcompression.dll)
https://coderator.net/c-experimental-serisi-native-kutuphane-derliyoruz/ (helpful to get the setup done)
https://github.com/dotnet/corert/issues/7096 (solved missing ubuntu lib