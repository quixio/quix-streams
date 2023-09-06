import argparse
import os
import shutil
import subprocess
import sys


def parse_args():
    parser = argparse.ArgumentParser(description="Build C# to native for Linux")
    parser.add_argument(
        "--no-interop",
        help="If set, don't copy the generated interop layer to native python folder. "
        "Useful for when testing existing manual modifications without "
        "spending time on rebuilding.",
        action="store_true",
    )
    parser.add_argument(
        "--no-python",
        help="If set, don't copy the generated python wrapper to native python folder."
        "Useful when doing manual modifications to test fixes",
        action="store_true",
    )
    parser.add_argument(
        "--no-regen",
        help="If set, leave the interop results as they are. "
        "Useful when making manual modifications to either c# or python side"
        " to test fixes",
        action="store_true",
    )
    parser.add_argument(
        "--debug",
        help="set -c configuration for build options",
        action="store_true",
    )
    return parser.parse_args()


def build_streaming_project(csharpfolder, framework, streamingoutpath):
    print("Build streaming project")
    subprocess.run(
        f"dotnet publish "
        f"{csharpfolder}/QuixStreams.Streaming/QuixStreams.Streaming.csproj "
        f"{framework} "
        f"-c Python "
        f"-o {streamingoutpath}",
        shell=True,
        check=True,
    )


def build_and_run_interop_generator(
    interopfolder,
    streamingoutpath,
    interopoutput,
    interopconfig,
    noregen,
    dotnetruntime,
):
    if not noregen:
        print("Build interop generator")
        interopgeneratoroutput = (
            f"{interopfolder}/Quix.InteropGenerator/bin/Publish/{dotnetruntime}"
        )
        subprocess.run(
            f"dotnet publish "
            f"{interopfolder}/Quix.InteropGenerator/Quix.InteropGenerator.csproj "
            f"-c release "
            f"-o {interopgeneratoroutput}",
            shell=True,
            check=True,
        )

        print("Run interop generator")
        subprocess.run(
            f"{os.path.abspath(interopgeneratoroutput)}/Quix.InteropGenerator "
            f'-a "{streamingoutpath}/QuixStreams.Streaming.dll" '
            f'-o "{interopoutput}" '
            f'-c "{interopconfig}"',
            shell=True,
            check=True,
        )
    else:
        print("Not regenerating interop projects due to --no-regen flag")


def build_interop_projects(
    interopoutputcsharp, configuration, dotnetruntime, dest_platform, nointerop
):
    if not nointerop:
        print("Cleaning interop folder...")
        shutil.rmtree(dest_platform, ignore_errors=True)

        print("Build interop projects")
        for subdir in os.listdir(interopoutputcsharp):
            interop_project_dir = f"{interopoutputcsharp}/{subdir}"
            dest_platform_subdir = f"{dest_platform}/{subdir}"
            cmd = (
                f"dotnet publish {interop_project_dir}/{subdir}.csproj "
                f"/p:NativeLib=Shared "
                f"/p:SelfContained=true "
                f"-r {dotnetruntime} "
                f"{configuration} "
                f"-o {dest_platform_subdir}"
            )
            subprocess.run(
                cmd,
                shell=True,
                check=True,
            )
    else:
        print("Not recompiling interop due to --no-interop flag")


def copy_python_interop(interopoutput, dest_python, nopython):
    if not nopython:
        print("Cleaning python folder...")
        shutil.rmtree(dest_python, ignore_errors=True)

        print(f"Copying python interop to native dir: {dest_python}")
        shutil.copytree(f"{interopoutput}/Python", dest_python)
        count = 0
        for root, dirs, files in os.walk(dest_python):
            count += len(files)

        print(f"{count} files copied.")
    else:
        print("Not copying python due to --no-python flag")


def main():
    args_ = parse_args()
    archname = os.uname().machine
    python_platform = f"{os.uname().sysname}-{archname}".lower()
    if archname == "aarch64":
        dotnet_runtime = "linux-arm64"
    elif archname == "x86_64":
        dotnet_runtime = "linux-x64"
    else:
        print(f"Not yet supported architecture {archname}")
        sys.exit(1)

    print(
        f"Building for linux architecture {archname} with dotnet "
        f"runtime id {dotnet_runtime} with python platform {python_platform}"
    )

    interopfolder = "../../../InteropGenerator"
    csharpfolder = "../../../CsharpClient"
    pythonfolder = "../../../PythonClient"
    streamingoutpath = (
        f"{csharpfolder}/QuixStreams.Streaming/bin/Publish/{dotnet_runtime}"
    )
    framework = "-f net8.0"

    build_streaming_project(csharpfolder, framework, streamingoutpath)

    interopoutput = f"{interopfolder}/InteropOutput"
    interopconfig = f"{interopfolder}/InteropConfig"

    build_and_run_interop_generator(
        interopfolder,
        streamingoutpath,
        interopoutput,
        interopconfig,
        args_.no_regen,
        dotnet_runtime,
    )

    dest = f"{pythonfolder}/src/quixstreams/native"
    dest_python = f"{dest}/Python"
    dest_platform = f"{dest}/{python_platform}"

    interopoutputcsharp = f"{interopoutput}/Csharp"

    configuration = (
        "-c release /p:DebugType=None /p:DebugSymbols=false"
        if not args_.debug
        else "-c debug"
    )
    build_interop_projects(
        interopoutputcsharp,
        configuration,
        dotnet_runtime,
        dest_platform,
        args_.no_interop,
    )
    copy_python_interop(interopoutput, dest_python, args_.no_python)


if __name__ == "__main__":
    main()
