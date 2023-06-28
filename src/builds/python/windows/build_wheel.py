import os
import shutil
import subprocess

python_folder = "../../../PythonClient"
directories_to_remove = [
    os.path.join(python_folder, "build"),
    os.path.join(python_folder, "dist"),
    os.path.join(python_folder, "src", "quixstreams.egg-info"),
]

for directory in directories_to_remove:
    if os.path.exists(directory):
        shutil.rmtree(directory)

os.chdir(python_folder)
subprocess.run(
    ["python", "setup.py", "sdist", "bdist_wheel", "--plat-name", "win_amd64"],
    check=True,
)
