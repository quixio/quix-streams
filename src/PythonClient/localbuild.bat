:: Depending on what you have installed, enable the line that is relevant
RMDIR "./lib/quixstreaming/dotnet" /S /Q
set buildCmd="dotnet publish ../CsharpClient/Quix.Sdk.Streaming/Quix.Sdk.Streaming.csproj -c python  /p:DebugType=None --runtime RUNTIMEID -o ./lib/quixstreaming/dotnet/RUNTIMEID"
:: use this one for debugging
cmd /c %buildCmd:RUNTIMEID=win-x64%
:: use these for actually creating a locally built package
:: cmd /c %buildCmd:RUNTIMEID=win-x86%
:: cmd /c %buildCmd:RUNTIMEID=linux-x64%
:: cmd /c %buildCmd:RUNTIMEID=osx-x64%

:: uncomment below if you wish to install package
:: cd lib
:: pip install wheel
:: python setup.py sdist bdist_wheel

:: cd ../env/lib/site-packages
:: for /f %%i in ('dir /a:d /s /b *QuixStreaming*') do echo rd /s /q %%i
:: cd ../../..

:: cd lib/dist

:: for /f "tokens=1,2,3,4" %%A in ('dir /s ^| findstr whl') do pip install %%D

:: cd ../..