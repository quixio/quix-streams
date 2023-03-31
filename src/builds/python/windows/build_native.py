import os
import sys
import shutil
import subprocess
import platform

def parse_arguments(args):
    options = {
        'nointerop': False,
        'nopython': False,
        'noregen': False,
        'configuration': "-c release /p:DebugType=None /p:DebugSymbols=false",
    }

    for arg in args:
        if arg == "--nointerop":
            options['nointerop'] = True
        elif arg == "--nopython":
            options['nopython'] = True
        elif arg == "--debug":
            options['configuration'] = "-c debug"
        elif arg == "--noregen":
            options['noregen'] = True

    return options

def build_streaming_project(csharpfolder, framework, streamingoutpath):
    print("Build streaming project")
    subprocess.run(f"dotnet publish {csharpfolder}/QuixStreams.Streaming/QuixStreams.Streaming.csproj {framework} -c release -o {streamingoutpath}", shell=True, check=True)

def build_and_run_interop_generator(interopfolder, streamingoutpath, interopoutput, interopconfig, noregen, dotnetruntime):
    if not noregen:
        print("Build interop generator")
        interopgeneratoroutput = f"{interopfolder}/Quix.InteropGenerator/bin/Publish/{dotnetruntime}"
        subprocess.run(f"dotnet publish {interopfolder}/Quix.InteropGenerator/Quix.InteropGenerator.csproj -c release -o {interopgeneratoroutput}", shell=True, check=True)
        
        print("Run interop generator")
        subprocess.run(f"{os.path.abspath(interopgeneratoroutput)}/Quix.InteropGenerator.exe -a \"{streamingoutpath}/QuixStreams.Streaming.dll\" -o \"{interopoutput}\" -c \"{interopconfig}\"", shell=True, check=True)
    else:
        print("Not regenerating interop projects due to --noregen flag")

def build_interop_projects(interopoutputcsharp, configuration, dotnetruntime, destPlatform, nointerop):
    if not nointerop:
        print("Cleaning interop folder...")
        shutil.rmtree(destPlatform, ignore_errors=True)

        print("Build interop projects")
        for subdir in os.listdir(interopoutputcsharp):
            interop_project_dir = f"{interopoutputcsharp}/{subdir}"
            dest_platform_subdir = f"{destPlatform}/{subdir}"
            subprocess.run(f"dotnet publish {interop_project_dir}/{subdir}.csproj /p:NativeLib=Shared /p:SelfContained=true {configuration} -r {dotnetruntime} -o {dest_platform_subdir}", shell=True, check=True)
    else:
        print("Not recompiling interop due to --nointerop flag")

def copy_python_interop(interopoutput, destPython, nopython):
    if not nopython:
        print("Cleaning python folder...")
        shutil.rmtree(destPython, ignore_errors=True)

        print("Copying python interop to native")
        shutil.copytree(f"{interopoutput}/Python", destPython)
        count = 0
        for root, dirs, files in os.walk(destPython):
            count += len(files)

        print(f"{count} files copied.")
    else:
        print("Not copying python due to --nopython flag")


def main():
    options = parse_arguments(sys.argv[1:])
    
    dotnetruntime = "win-x64"
    python_platform = f"{platform.system().lower()}-{platform.machine().lower()}"
    
    interopfolder = "../../../InteropGenerator"
    csharpfolder = "../../../CsharpClient"
    pythonfolder = "../../../PythonClient"
    streamingoutpath = f"{csharpfolder}/QuixStreams.Streaming/bin/Publish/{dotnetruntime}"
    framework = "-f net8.0"

    build_streaming_project(csharpfolder, framework, streamingoutpath)

    interopoutput = f"{interopfolder}/InteropOutput"
    interopconfig = f"{interopfolder}/InteropConfig"
    
    build_and_run_interop_generator(interopfolder, streamingoutpath, interopoutput, interopconfig, options['noregen'], dotnetruntime)

    dest = f"{pythonfolder}/src/quixstreams/native"
    destPython = f"{dest}/Python"
    destPlatform = f"{dest}/{python_platform}"
    
    interopoutputcsharp = f"{interopoutput}/Csharp"
    
    build_interop_projects(interopoutputcsharp, options['configuration'], dotnetruntime, destPlatform, options['nointerop'])
    copy_python_interop(interopoutput, destPython, options['nopython'])


if __name__ == "__main__":
    main()