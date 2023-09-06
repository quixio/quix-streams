import subprocess
import sys
import os
import re

print("Dependency check started")

def check_installed_package(package):
    try:
        subprocess.run([sys.executable, '-m', 'pip', 'show', package], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


def install_package(package):
    subprocess.run([sys.executable, '-m', 'pip', 'install', package], check=True)


# Check for wheel package
print("... checking wheel package dependency")
if not check_installed_package('wheel'):
    install_package('wheel')


def check_installed_program_version(program, version_pattern):
    try:
        result = subprocess.run([program, '--version'], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, check=True, text=True)
        version = result.stdout.strip()
        if re.match(version_pattern, version):
            return True
        else:
            print(f"Warning: Found version {version}, but expected {version_pattern}")
            return False
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False

def export_statements(statements):
    zshrc_file = os.path.expanduser('~/.zshrc')

    if not os.path.exists(zshrc_file):
        # Create the file if it does not exist
        open(zshrc_file, 'w').close()
        
    with open(zshrc_file, 'r') as zshrc:
        zshrc_lines = zshrc.readlines()

    # Check if each export statement is already present in the file
    for statement in statements:
        if statement not in zshrc_lines:
            with open(zshrc_file, 'a') as zshrc:
                zshrc.write(statement)

    # exporting to current shell also, without reloading shell. shell reload in cases I tested caused script termination or build hangs
    for statement in statements:
        subprocess.run(f'{statement}', shell=True)

def install_dotnet(version, channel):
    curl_cmd = ['curl', '-L', 'https://dot.net/v1/dotnet-install.sh', '-o', 'dotnet-install.sh']
    subprocess.run(curl_cmd, check=True)

    os.chmod('dotnet-install.sh', 0o755)
    install_dir = os.path.join(os.getcwd(), '.dotnet')
    dotnet_install_cmd = ['./dotnet-install.sh', '--version', version, '--channel', channel, '--install-dir', install_dir]
    subprocess.run(dotnet_install_cmd, check=True)

    os.remove('dotnet-install.sh')

    statements_to_export = [
        f"export DOTNET_ROOT={install_dir}\n",
        f"export PATH=$PATH:{install_dir}:{install_dir}/tools\n"
    ]

    export_statements(statements_to_export)

# Check for .NET SDK
print("... checking dotnet dependency")
dotnet_major_version = '8'
expected_version = f'{dotnet_major_version}.0.100-preview.2.23157.25'
expected_version_channel = f'{dotnet_major_version}.0.1xx'
exact_version_pattern = rf'^{re.escape(expected_version)}$'
similar_version_pattern = rf'^{dotnet_major_version}\.\d+\.\d+$'
if os.path.exists("./.dotnet"):
    if not check_installed_program_version('./.dotnet/dotnet', exact_version_pattern):
        if not check_installed_program_version('./.dotnet/dotnet', similar_version_pattern):
            print(f"Installing dotnet")
            install_dotnet(expected_version, expected_version_channel)
else:
    if not check_installed_program_version('dotnet', exact_version_pattern):
        if not check_installed_program_version('dotnet', similar_version_pattern):
            print(f"Installing dotnet")
            install_dotnet(expected_version, expected_version_channel)

def install_or_update_brew():
    print("... checking brew dependency")
    try:
        subprocess.run(['brew', 'update'], check=True)
    except (FileNotFoundError, subprocess.CalledProcessError):
        subprocess.run(['/bin/bash', '-c', "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"], check=True)
        brew_install_loc = '/opt/homebrew/bin' if os.uname().machine == 'arm64' else '/usr/local/bin'
        with open(os.path.expanduser('~/.zshrc'), 'a') as zshrc:
            zshrc.write(f"export PATH={brew_install_loc}:$PATH\n")
            subprocess.run(['zsh', '-c', f'source {os.path.expanduser("~/.zshrc")}'], shell=True, check=True)

install_or_update_brew()

print("... checking librdkafka dependency")
subprocess.run(['brew', 'install', 'librdkafka'], check=True)
print("Dependency check finished")