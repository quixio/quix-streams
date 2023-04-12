import subprocess
import sys
import re

def check_installed_package(package):
    try:
        subprocess.run([sys.executable, '-m', 'pip', 'show', package], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        return True
    except subprocess.CalledProcessError:
        return False

def install_package(package):
    subprocess.run([sys.executable, '-m', 'pip', 'install', package], check=True)

# Check for wheel package
if not check_installed_package('wheel'):
    install_package('wheel')


def check_installed_program_version(program, version_pattern):
    try:
        result = subprocess.run([program, '--version'], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, check=True, text=True)
        version = result.stdout.strip()
        if re.match(version_pattern, version):
            return True
        else:
            return False
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False

# Check for .NET 8 SDK
if not check_installed_program_version('dotnet', r'^8\.\d+\.\d+'):
    print('.NET 8 SDK not found or incorrect version. Please install the correct .NET 8 SDK version.')
    sys.exit(1)