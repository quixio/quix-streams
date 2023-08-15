import argparse
import os
import shutil
import subprocess
import sys


def parse_args():
    parser = argparse.ArgumentParser(description="Build C# to native for Mac")
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


def build_streaming_project(dotnet_command, csharpfolder, framework, streamingoutpath):
    print("Build streaming project")
    subprocess.run(
        f"{dotnet_command} publish "
        f"{csharpfolder}/QuixStreams.Streaming/QuixStreams.Streaming.csproj "
        f"{framework} "
        f"-c Python "
        f"-o {streamingoutpath}",
        shell=True,
        check=True,
    )


def build_and_run_interop_generator(
    dotnet_command,
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
            f"{dotnet_command} publish "
            f"{interopfolder}/Quix.InteropGenerator/Quix.InteropGenerator.csproj "
            f"-c Python "
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
    dotnet_command, interopoutputcsharp, configuration, dotnetruntime, dest_platform, nointerop
):
    if not nointerop:
        print("Cleaning interop folder...")
        shutil.rmtree(dest_platform, ignore_errors=True)

        print("Build interop projects")
        for subdir in os.listdir(interopoutputcsharp):
            interop_project_dir = f"{interopoutputcsharp}/{subdir}"
            dest_platform_subdir = f"{dest_platform}/{subdir}"
            subprocess.run(
                f"{dotnet_command} publish "
                f"{interop_project_dir}/{subdir}.csproj "
                f"/p:NativeLib=Shared "
                f"/p:SelfContained=true "
                f"{configuration} "
                f"-r {dotnetruntime} "
                f"-o {dest_platform_subdir}",
                shell=True,
                check=True,
            )

        collect_and_copy_dylibs(dest_platform)
    else:
        print("Not recompiling interop due to --no-interop flag")


def copy_python_interop(interopoutput, dest_python, nopython):
    if not nopython:
        print("Cleaning python folder...")
        shutil.rmtree(dest_python, ignore_errors=True)

        print(f"Copying python interop to native: {dest_python}")
        shutil.copytree(f"{interopoutput}/Python", dest_python)
        count = 0
        for root, dirs, files in os.walk(dest_python):
            count += len(files)

        print(f"{count} files copied.")
    else:
        print("Not copying python due to --no-python flag")


def collect_and_copy_dylibs(dest_platform):
    searchpaths = ("/usr/local/Cellar", "/opt/homebrew/Cellar", "/usr/local/lib")

    def run_cmd(command):
        return subprocess.check_output(command, shell=True, text=True).strip()

    def copy_dylib_deps(dylib_filepath):
        dylib_filename = os.path.basename(dylib_filepath)
        dylib_file_folder = os.path.dirname(dylib_filepath)
        files_to_inspect = [dylib_filepath]
        file_names_to_inspect = [dylib_filename]

        copied_dylibs = []
        i = 0
        while i < len(files_to_inspect):
            file_to_inspect = files_to_inspect[i]
            file = os.path.basename(file_to_inspect)
            print(f"Handling {file}")
            print(f"\tOriginal location: {file_to_inspect}")

            if file_to_inspect.startswith("/usr/lib/"):
                print("\tSystem file, skipping")
                i += 1
                continue
            if file_to_inspect.startswith("/System/Library/"):
                print("\tSystem file, skipping")
                i += 1
                continue

            file_new_dest = os.path.join(dylib_file_folder, file)
            if os.path.isfile(file_new_dest):
                print("\tFile already copied to necessary location")
                file_to_inspect = file_new_dest
                copied_dylibs.append(file_new_dest)
            else:
                if os.path.isfile(file_to_inspect):
                    print("\tFound at original location")
                else:
                    print("\tNeeds searching, didn't find at original location.")
                    found = False
                    for searchpath in searchpaths:
                        print(f"\t\tSearching at {searchpath}")
                        try:
                            search_output = run_cmd(f"find {searchpath} -name {file}")
                        except (
                            subprocess.CalledProcessError
                        ):  # No such file or directory exception
                            continue
                        if search_output:
                            file_to_inspect = search_output.split("\n")[0]
                            found = True
                            print(f"\t\t\t{file} found at {file_to_inspect}")
                            break
                    if not found:
                        print("\t\tDid not find anywhere")
                        i += 1
                        continue

                print(f"\tCopying file to {file_new_dest}")
                os.system(f"cp {file_to_inspect} {file_new_dest}")
                copied_dylibs.append(file_new_dest)
                print("\tCopy finished")

            output = run_cmd(
                f"otool "
                f"-L {file_to_inspect} "
                f"| grep 'dylib (' "
                f"| cut -f 2 "
                f"| cut -d ' ' -f 1"
            )
            for line in output.split("\n"):
                linefilename = os.path.basename(line)
                if linefilename not in file_names_to_inspect:
                    files_to_inspect.append(line)
                    file_names_to_inspect.append(linefilename)
            i += 1

    def add_top_rpath(dylib_filepath):
        dylib_filename = os.path.basename(dylib_filepath)
        dylib_file_folder = os.path.dirname(dylib_filepath)
        dylib_file_folder_name = os.path.basename(dylib_file_folder)
        dylib_filename_next, _ = os.path.splitext(dylib_filename)

        if dylib_filename_next == dylib_file_folder_name:
            os.system(f"install_name_tool -add_rpath @loader_path {dylib_filepath}")
            print(f"Added @loader_path to {dylib_filepath}")

    dylibs_output = run_cmd(f"find {dest_platform} -name '*.dylib'")
    for dylib_filepath in dylibs_output.split("\n"):
        copy_dylib_deps(dylib_filepath)
        add_top_rpath(dylib_filepath)


def main():
    args_ = parse_args()
    archname = os.uname().machine
    python_platform = f"{os.uname().sysname}-{os.uname().machine}".lower()
    dotnet_command = "dotnet"

    if archname == "arm64":
        dotnetruntime = "osx.11.0-arm64"
    elif archname == "x86_64":
        dotnetruntime = "osx-x64"
    else:
        print(f"Not yet supported architecture {archname}")
        sys.exit(1)

    print(
        f"Building for Mac architecture {archname} "
        f"with dotnet runtime id {dotnetruntime} "
        f"with python platform {python_platform}"
    )

    extra_install_dir = os.path.join(os.getcwd(), '.dotnet')

    if os.path.exists(extra_install_dir):
        print("Using local folder .dotnet, as present")
        dotnet_command=f"{extra_install_dir}/dotnet"

    interopfolder = "../../../InteropGenerator"
    csharpfolder = "../../../CsharpClient"
    pythonfolder = "../../../PythonClient"
    streamingoutpath = (
        f"{csharpfolder}/QuixStreams.Streaming/bin/Publish/{dotnetruntime}"
    )
    framework = "-f net8.0"

    build_streaming_project(dotnet_command, csharpfolder, framework, streamingoutpath)

    interopoutput = f"{interopfolder}/InteropOutput"
    interopconfig = f"{interopfolder}/InteropConfig"

    build_and_run_interop_generator(
        dotnet_command,
        interopfolder,
        streamingoutpath,
        interopoutput,
        interopconfig,
        args_.no_regen,
        dotnetruntime,
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
        dotnet_command,
        interopoutputcsharp,
        configuration,
        dotnetruntime,
        dest_platform,
        args_.no_interop,
    )

    copy_python_interop(interopoutput, dest_python, args_.no_python)


if __name__ == "__main__":
    main()
