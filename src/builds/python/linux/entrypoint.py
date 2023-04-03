import subprocess
import sys
import os
import glob
import shutil

def run_script(script_name):
    try:
        subprocess.run([sys.executable, script_name], check=True)
    except subprocess.CalledProcessError as e:
        sys.exit(e.returncode)

os.chdir('/QuixStreams/builds/python/linux')

run_script('build_native.py')
run_script('build_wheel.py')

build_result_dir = '/build-result'
os.makedirs(build_result_dir, exist_ok=True)
python_folder = '../../../PythonClient'
os.chdir(python_folder)

whl_files = glob.glob('./dist/*.whl')

for whl_file in whl_files:
    shutil.copy(whl_file, build_result_dir)