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
if archname == "aarch64":
    platname = "manylinux_2_28_aarch64"
elif archname == "x86_64":
    platname = "manylinux2014_x86_64"
else:
    print(f"Not yet supported architecture {archname}")
    sys.exit(1)

os.chdir(python_folder)
subprocess.run(
    ["python3", "setup.py", "sdist", "bdist_wheel", "--plat-name", platname], check=True
)
