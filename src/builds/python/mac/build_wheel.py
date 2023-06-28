import os
import shutil
import subprocess
import sys

python_folder = "../../../PythonClient"
directories_to_remove = [
    os.path.join(python_folder, "build"),
    os.path.join(python_folder, "dist"),
    os.path.join(python_folder, "src", "quixstreams.egg-info"),
]

for directory in directories_to_remove:
    if os.path.exists(directory):
        shutil.rmtree(directory)

archname = os.uname().machine
if archname == "arm64":
    # based on python3 -m pip debug --verbose | grep arm64
    platname = "macosx_11_0_arm64"
elif archname == "x86_64":
    # https://learn.microsoft.com/en-us/dotnet/core/rid-catalog
    # we build it using osx-x64, which has (Minimum OS version is macOS 10.12 Sierra)
    # therefore defaulting to
    platname = "macosx_10_12_intel"
else:
    print(f"Not yet supported architecture {archname}")
    sys.exit(1)

os.chdir(python_folder)
subprocess.run(
    ["python3", "setup.py", "sdist", "bdist_wheel", "--plat-name", platname], check=True
)
