#!/bin/bash

archname=$(uname -m)
pythonplatform=$(python3 -c 'import platform;plat=platform.uname();print(f"{plat.system}-{plat.machine}".lower())')
if [ $archname == 'aarch64' ]; then
    dotnetruntime=linux-arm64
elif [ $archname == 'x86_64' ]; then
    dotnetruntime=linux-x64
else
  echo "Not yet supported architecture $archname"
  exit 1
fi

echo "Building for linux architecture $archname with dotnet runtime id $dotnetruntime with python platform $pythonplatform"

interopfolder=../../../InteropGenerator
csharpfolder=../../../CsharpClient
pythonfolder=../../../PythonClient
streamingoutpath=$csharpfolder/QuixStreams.Streaming/bin/Publish/linux
echo build streaming
dotnet publish $csharpfolder/QuixStreams.Streaming/QuixStreams.Streaming.csproj -f net7.0 -c release -o $streamingoutpath
if [[ $? != 0 ]]; then
    echo "Failed dotnet build QuixStreams.Streaming.csproj"
    exit 2
fi

interopgeneratoroutput=$interopfolder/Quix.InteropGenerator/bin/Publish/linux
echo build interop generator
dotnet publish $interopfolder/Quix.InteropGenerator/Quix.InteropGenerator.csproj -c release -o $interopgeneratoroutput
if [[ $? != 0 ]]; then
    echo "Failed dotnet publish Quix.InteropGenerator.csproj"
    exit 3
fi

echo run interop generator
interopoutput=$interopfolder/InteropOutput
interopconfig=$interopfolder/InteropConfig
"$interopgeneratoroutput/Quix.InteropGenerator" -a "$streamingoutpath/QuixStreams.Streaming.dll" -o "$interopoutput"  -c "$interopconfig"
if [[ $? != 0 ]]; then
    echo "Failed InteropGenerator run"
    exit 4
fi

dest=$pythonfolder/src/quixstreams/native
destPython=$dest/Python
destPlatform=$dest/$pythonplatform
echo Cleaning native folders...
rm -rf "$destPython"
rm -rf "$destPlatform"

echo Build interop
interopoutputcsharp=$interopoutput/Csharp
for assemblydir in $interopoutputcsharp/* ; do
    assemblyname=$(basename $assemblydir)
    dotnet publish $assemblydir/$assemblyname.csproj /p:NativeLib=Shared /p:SelfContained=true -r $dotnetruntime -c Debug -o $destPlatform/$assemblyname
    if [[ $? != 0 ]]; then
        echo "Failed dotnet publish $assemblyname.csproj"
        exit 5
    fi
done

echo Copy python interop to native
cp -R  "$interopoutput/Python/" "$destPython/"
