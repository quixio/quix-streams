import os
import shutil
import glob

python_folder = '../../../PythonClient'
dest = './build-result/'

if os.path.exists(dest):
    shutil.rmtree(dest)

os.makedirs(dest)

whl_files = glob.glob(os.path.join(python_folder, 'dist', '*.whl'))

for whl_file in whl_files:
    shutil.copy(whl_file, dest)