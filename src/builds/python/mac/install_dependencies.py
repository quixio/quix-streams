import subprocess
import sys
import os
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


def install_dotnet(version, channel):
    curl_cmd = ['curl', '-L', 'https://dot.net/v1/dotnet-install.sh', '-o', 'dotnet-install.sh']
    subprocess.run(curl_cmd, check=True)

    os.chmod('dotnet-install.sh', 0o755)

    dotnet_install_cmd = ['./dotnet-install.sh', '--version', version, '--channel', channel]
    subprocess.run(dotnet_install_cmd, check=True)

    with open(os.path.expanduser('~/.zshrc'), 'a') as zshrc:
        zshrc.write("export DOTNET_ROOT=$HOME/.dotnet\n")
        zshrc.write("export PATH=$PATH:$HOME/.dotnet:$HOME/.dotnet/tools\n")

    subprocess.run(['source', os.path.expanduser('~/.zshrc')], shell=True, check=True)


# Check for .NET SDK
if not check_installed_program_version('dotnet', r'^8\.\d+\.\d+'):
    print(f"Installing dotnet")
    install_dotnet('8.0.100-preview.2.23157.25', '8.0.1xx')

def install_or_update_brew():
    try:
        subprocess.run(['brew', 'update'], check=True)
    except subprocess.CalledProcessError:
        subprocess.run(['/bin/bash', '-c', "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"], check=True)
        brew_install_loc = '/opt/homebrew/bin' if os.uname().machine == 'arm64' else '/usr/local/bin'
        with open(os.path.expanduser('~/.zshrc'), 'a') as zshrc:
            zshrc.write(f"export PATH={brew_install_loc}:$PATH\n")
        subprocess.run(['source', os.path.expanduser('~/.zshrc')], shell=True, check=True)

install_or_update_brew()

subprocess.run(['brew', 'install', 'librdkafka'], check=True)