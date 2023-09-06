import sys
import subprocess
import re
import os
import shutil
import fileinput
from typing import List

version = "0.6.0.0"
informal_version = "0.6.0.0-dev1"
nuget_version = "0.6.0.0-dev1"


def updatecsproj(projfilepath):
    # Check if the path exists
    if not os.path.exists(projfilepath):
        raise Exception(f"Path '{projfilepath}' does not exist")

    with fileinput.input(projfilepath, inplace=True) as file:
        for line in file:
            # Replace the old text with the new text
            new_line = re.sub("<Version>.*</Version>", f"<Version>{version}</Version>", line.rstrip())
            new_line = re.sub("<AssemblyVersion>.*</AssemblyVersion>", f"<AssemblyVersion>{version}</AssemblyVersion>", new_line)
            new_line = re.sub("<FileVersion>.*</FileVersion>", f"<FileVersion>{version}</FileVersion>", new_line)
            new_line = re.sub("<InformationalVersion>.*</InformationalVersion>", f"<InformationalVersion>{informal_version}</InformationalVersion>", new_line)
            print(new_line)

def run_command(cmd: List[str]) -> int:
    # Run a command and capture its output in real-time
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)

    # Print the output as it becomes available
    while True:
        line = proc.stdout.readline()
        if line == '':
            break
        print(line.strip())

    # Wait for the command to finish and get its return code
    return_code = proc.wait()

    if (return_code != 0):
        raise Exception("Failed to run command")

    return return_code


def get_csharp_root_path():
    file_folder = os.path.abspath(os.path.dirname(__file__))
    relative_path_root = "../../../CsharpClient"
    abs_path_root = os.path.abspath(os.path.join(file_folder, relative_path_root))

    # Check if the path exists
    if os.path.exists(abs_path_root):
        return abs_path_root
    else:
        raise Exception(f"Path '{abs_path_root}' does not exist")

def get_output_path():
    file_folder = os.path.abspath(os.path.dirname(__file__))
    path = os.path.abspath(os.path.join(file_folder, "./build-result"))
    # Check if the path exists
    if os.path.exists(path):
        shutil.rmtree(path)
    return path
    

source_root_path = get_csharp_root_path()
result_root_path = get_output_path()

projects = [
    os.path.join(source_root_path, "QuixStreams.Streaming/QuixStreams.Streaming.csproj"),
    os.path.join(source_root_path, "QuixStreams.Telemetry/QuixStreams.Telemetry.csproj"),
    os.path.join(source_root_path, "QuixStreams.Kafka/QuixStreams.Kafka.csproj"),
    os.path.join(source_root_path, "QuixStreams.Kafka.Transport/QuixStreams.Kafka.Transport.csproj"),
    os.path.join(source_root_path, "QuixStreams.State/QuixStreams.State.csproj")
]

print(f"Updating project files with version")
for proj_csproj in projects:
    updatecsproj(proj_csproj)
print(f"Updated project files with version")

print("")
print("")
print(f"Packaging projects")
for proj_csproj in projects:
    run_command(f"dotnet pack {proj_csproj} -c release /p:PackageVersion={nuget_version} --output {result_root_path}".split())
print(f"Packaging projects")


print(sys.argv)