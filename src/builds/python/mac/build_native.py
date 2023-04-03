import os
import sys
import shutil
import subprocess
import platform

def parse_arguments(args):
    options = {
        'no-interop': False, # When set to true, does not copy the generated interop layer to native python folder. Useful for when testing existing manual modifications without spending time on rebuilding.
        'no-python': False, # When set to true, does not copy the generated python wrapper to native python folder. Useful when doing manual modifications to test fixes
        'no-regen': False, # When set to true, leave the interop results as they are. Useful when making manual modifications to either c# or python side to test fixes.
        'configuration': "-c release /p:DebugType=None /p:DebugSymbols=false",
    }

    for arg in args:
        if arg == "--no-interop":
            options['no-interop'] = True
        elif arg == "--no-python":
            options['no-python'] = True
        elif arg == "--no-regen":
            options['no-regen'] = True
        elif arg == "--debug":
            options['configuration'] = "-c debug"

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
        subprocess.run(f"{os.path.abspath(interopgeneratoroutput)}/Quix.InteropGenerator -a \"{streamingoutpath}/QuixStreams.Streaming.dll\" -o \"{interopoutput}\" -c \"{interopconfig}\"", shell=True, check=True)
    else:
        print("Not regenerating interop projects due to --no-regen flag")

def build_interop_projects(interopoutputcsharp, configuration, dotnetruntime, destPlatform, nointerop):
    if not nointerop:
        print("Cleaning interop folder...")
        shutil.rmtree(destPlatform, ignore_errors=True)

        print("Build interop projects")
        for subdir in os.listdir(interopoutputcsharp):
            interop_project_dir = f"{interopoutputcsharp}/{subdir}"
            dest_platform_subdir = f"{destPlatform}/{subdir}"
            subprocess.run(f"dotnet publish {interop_project_dir}/{subdir}.csproj /p:NativeLib=Shared /p:SelfContained=true {configuration} -r {dotnetruntime} -o {dest_platform_subdir}", shell=True, check=True)

        collect_and_copy_dylibs(destPlatform)
    else:
        print("Not recompiling interop due to --no-interop flag")

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
        print("Not copying python due to --no-python flag")

def collect_and_copy_dylibs(destPlatform):
    
    searchpaths = ('/usr/local/Cellar', '/opt/homebrew/Cellar', '/usr/local/lib')

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
                        search_output = run_cmd(f"find {searchpath} -name {file}")
                        if search_output:
                            file_to_inspect = search_output.split('\n')[0]
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

            output = run_cmd(f"otool -L {file_to_inspect} | grep 'dylib (' | cut -f 2 | cut -d ' ' -f 1")
            for line in output.split('\n'):
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

    dylibs_output = run_cmd(f"find {destPlatform} -name '*.dylib'")
    for dylib_filepath in dylibs_output.split('\n'):
        copy_dylib_deps(dylib_filepath)
        add_top_rpath(dylib_filepath)


def main():
    options = parse_arguments(sys.argv[1:])
    
    archname = os.uname().machine
    python_platform = f'{os.uname().sysname}-{os.uname().machine}'.lower()

    if archname == 'arm64':
        dotnetruntime = 'osx.11.0-arm64'
    elif archname == 'x86_64':
        dotnetruntime = 'osx-x64'
    else:
        print(f'Not yet supported architecture {archname}')
        sys.exit(1)

    print(f'Building for Mac architecture {archname} with dotnet runtime id {dotnetruntime} with python platform {python_platform}')
    
    interopfolder = "../../../InteropGenerator"
    csharpfolder = "../../../CsharpClient"
    pythonfolder = "../../../PythonClient"
    streamingoutpath = f"{csharpfolder}/QuixStreams.Streaming/bin/Publish/{dotnetruntime}"
    framework = "-f net7.0"

    build_streaming_project(csharpfolder, framework, streamingoutpath)

    interopoutput = f"{interopfolder}/InteropOutput"
    interopconfig = f"{interopfolder}/InteropConfig"
    
    build_and_run_interop_generator(interopfolder, streamingoutpath, interopoutput, interopconfig, options['no-regen'], dotnetruntime)

    dest = f"{pythonfolder}/src/quixstreams/native"
    destPython = f"{dest}/Python"
    destPlatform = f"{dest}/{python_platform}"
    
    interopoutputcsharp = f"{interopoutput}/Csharp"
    
    build_interop_projects(interopoutputcsharp, options['configuration'], dotnetruntime, destPlatform, options['no-interop'])
    
    copy_python_interop(interopoutput, destPython, options['no-python'])


if __name__ == "__main__":
    main()