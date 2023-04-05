import subprocess
import sys
import os

def run_script(script_name, *args):
    script_path = os.path.join(os.getcwd(), script_name)
    command = [sys.executable, script_path] + list(args)
    subprocess.run(command, check=True)

try:
    run_script('install_dependencies.py')
    run_script('build_native.py', *sys.argv[1:])
    run_script('build_wheel.py')
    run_script('copy_build_result.py')
except subprocess.CalledProcessError as e:
    sys.exit(e.returncode)