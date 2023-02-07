@echo off
setlocal enabledelayedexpansion
set nointerop=False
set nopython=False
set noregen=False
set configuration=-c release /p:DebugType=None /p:DebugSymbols=false

set argCount=0
set argArray=

for %%i in (%*) do (
    set /A argCount+=1
    set argArray[!argCount!]=%%i
)

::echo Number of arguments: %argCount%
for /L %%i in (1,1,%argCount%) do (
	if "!argArray[%%i]!"=="--nointerop" (
		set nointerop=True
	) else if "!argArray[%%i]!"=="--nopython" (
		set nopython=True
	) else if "!argArray[%%i]!"=="--debug" (
		set configuration=-c debug
	) else if "!argArray[%%i]!"=="--noregen" (
		set noregen=True
	)
)

set dotnetruntime=win-x64
FOR /f %%i IN ('python -c "import platform;plat=platform.uname();print(f\"{plat.system}-{plat.machine}\".lower())"') do SET pythonplatform=%%i

set interopfolder=../../../InteropGenerator
set csharpfolder=../../../CsharpClient
set pythonfolder=../../../PythonClient
set streamingoutpath=%csharpfolder%/Quix.Sdk.Streaming/bin/Publish/win-x64
::echo build streaming
dotnet publish %csharpfolder%/Quix.Sdk.Streaming/Quix.Sdk.Streaming.csproj -c release -o %streamingoutpath%
if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%

set interopgeneratoroutput=%interopfolder%/Quix.InteropGenerator/bin/Publish/win-x64
set interopoutput=%interopfolder%/InteropOutput
set interopconfig=%interopfolder%/InteropConfig
if "%noregen%"=="False" (	
	echo build interop generator
	dotnet publish %interopfolder%/Quix.InteropGenerator/Quix.InteropGenerator.csproj -c release -o %interopgeneratoroutput%
	if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%

	echo run interop generator
	"%interopgeneratoroutput%/Quix.InteropGenerator.exe" -a "%streamingoutpath%/Quix.Sdk.Streaming.dll" -o "%interopoutput%"  -c "%interopconfig%"
	if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%
) else (
	echo Not regenerating interop projects due to --noregen flag	
)

set dest=%pythonfolder%/src/quixstreams/native
set destPython=%dest%/Python
set destPlatform=%dest%/%pythonplatform%


:: set variable here as inside if in some cases doesn't work
set interopoutputcsharp=%interopoutput%/Csharp
if "%nointerop%"=="False" (	
	echo Cleaning interop folder...
	if exist %destPlatform%\ rmdir /s /q "%destPlatform%\"
	if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%
	echo Build interop
	FOR /D %%f IN ("%interopoutputcsharp%/*") DO (
		echo dotnet publish %interopoutputcsharp%/%%f/%%f.csproj /p:NativeLib=Shared /p:SelfContained=true  %configuration% -r %dotnetruntime% -o %destPlatform%/%%f
		dotnet publish %interopoutputcsharp%/%%f/%%f.csproj /p:NativeLib=Shared /p:SelfContained=true  %configuration% -r %dotnetruntime% -o %destPlatform%/%%f
		if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%
	)
) else (
	echo Not recompiling interop due to --nointerop flag	
)
if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%

if "%nopython%"=="False" (	
	echo Cleaning python folder...
	if exist %destPython%\ rmdir /s /q "%destPython%\"
	echo Copy python interop to native
	xcopy /E /Y /D "%interopoutput%\Python\*.*" "%destPython%\"
) else (
	echo Not copying python due to --nopython flag
)