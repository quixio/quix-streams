#!/bin/sh
dotnet build ../CsharpClient/Quix.Sdk.Streaming/Quix.Sdk.Streaming.csproj /t:publish /restore /p:Configuration=PythonLocal /p:SelfContained=true /p:RuntimeIdentifier=linux-x64 /p:DebugType=None /p:PublishDir=../../PythonClient/lib/quixstreaming/dotnet/linux-x64 && docker-compose exec quix_sdk bash
