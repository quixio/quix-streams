# Who this readme is aimed at
If you're developing the library and wish to build locally, this readme is for you. Otherwise, check out 
https://test.pypi.org/project/quixstreams/ for latest ongoing dev versions or https://pypi.org/project/quixstreams/ for latest stable versions.

# Build for manylinux (x86_64 or arm64)
Go to the root of the repo in terminal, `/builds/python/linux`, then execute the relevant `buildx64.sh` or `buildarm64.sh` in a linux shell (wsl in windows)

## Frequent issues trying to get it to run on wsl

### 1) docker run just quits
Based on the resources linked below, you'll have to enable vsyscall=emulate
https://mail.python.org/pipermail/wheel-builders/2016-December/000239.html
(WSL) https://github.com/microsoft/WSL/issues/4694

For WSL, you enable it by editing your WSL config and restarting it:
```
%userprofile%\.wslconfig

[wsl2]
kernelCommandLine = vsyscall=emulate
```

To restart WSL:
```
wsl --shutdown
```

# Build for windows
Go to the root of the repo in terminal, `/builds/python/windows`, then execute the relevant `buildx64.sh` or `buildarm64.sh` in a linux shell (wsl in windows)

# Build for mac-osx-intel (x86_64)
Assuming you have a mac available, with nothing installed other than the OS

## Install dependencies
install dotnet
```
curl -L https://dot.net/v1/dotnet-install.sh -o dotnet-install.sh
chmod +x ./dotnet-install.sh
./dotnet-install.sh --version latest
echo "export DOTNET_ROOT=$HOME/.dotnet" >> ~/.zshrc
echo "export PATH=$PATH:$HOME/.dotnet:$HOME/.dotnet/tools" >> ~/.zshrc
source ~/.zshrc
```

install librdkafka binaries
```
brew install librdkafka
```

## Start build
Navigate to build_native_linux.sh folder 
```
cd WHEREVER_YOUR_ROOT_IS/Quix.Streams/PythonClient
. ./build_native_mac.sh
. ./build_wheel_mac.sh
```

## install built wheel directly
python3 -m pip uninstall quixstreams -y && python3 -m pip install ./dist/quixstreams-0.1.0-py3-none-macosx_10_12_intel.whl --user

# Build for mac-osx (arm/apple silicon - WIP)
Assuming you have a mac available, with nothing installed other than the OS

## Install dependencies
install dotnet
```
curl -L https://dot.net/v1/dotnet-install.sh -o dotnet-install.sh
chmod +x ./dotnet-install.sh
./dotnet-install.sh --version 8.0.100-alpha.1.23055.13 --channel 8.0.1xx
echo "export DOTNET_ROOT=$HOME/.dotnet" >> ~/.zshrc
echo "export PATH=$PATH:$HOME/.dotnet:$HOME/.dotnet/tools" >> ~/.zshrc
source ~/.zshrc
```

install librdkafka binaries
```
brew install librdkafka
```

## Start build
Navigate to build_native_linux.sh folder 
```
cd WHEREVER_YOUR_ROOT_IS/Quix.Streams/PythonClient
. ./build_native_mac.sh
. ./build_wheel_mac.sh

```

## install built wheel directly
python3 -m pip uninstall quixstreams -y && python3 -m pip install ./dist/quixstreams-0.1.0-py3-none-macosx_11_0_arm64.whl --user

# Run pypiserver locally to test hosting

Start your pypi server
```
docker run -p 8080:8080 pypiserver/pypiserver  -P . -a . --disable-fallback
```

You can verify it is running correctly by going to `http://localhost:8080/`.

Upload wheel files from a folder:
```
twine upload -p . -u . --skip-existing -r local --repository-url http://localhost:8080 ./*
```

You can use the website (`http://localhost:8080/simple/`) or the command below the list packages that you installed
```
pip search --index=http://localhost:8080 quixstreams
```

Install using the local registry:
```
python3 -m pip install quixstreams --index=http://localhost:8080/simple --extra-index=https://pypi.org/simple
```
Note: If you want to make sure the package works on its own, suggest `brew uninstall librdkafka`

# Test wheel

## Test content
The absolute minimum to test the installed package use the following `.py` file content:
```
echo "import quixstreams as qx" >> main.py
echo "client = qx.KafkaStreamingClient('127.0.0.1:9092', None)" >> main.py
echo "client.create_topic_producer('testinstall')" >> main.py
python3 main.py
```
Note: unless you have a local kafka running, this is expected to fail by logging Kafka producer exceptions.
Note2: On OSX, `DYLD_PRINT_APIS=1 DYLD_PRINT_LIBRARIES=1 python3 main.py` can be helpful for debugging load errors

## Test linux-x86_64
```
docker run -it \
    --platform=linux/amd64 \
    --entrypoint /bin/bash \
    --network=host \
    --volume /mnt/f/Work/source/Quix.Streams/builds/python/linux/build-result:/build-result \
    --volume /mnt/f/Work/source/Quix.Streams/PythonClient/tests/quixstreams/manual:/manual \
    python:3.11.1-slim-buster
```

## Test linux-arm64
```
docker run -it \
    --platform=linux/arm64/v8 \
    --entrypoint /bin/bash \
    --network=host \
    python:3.11.1-slim-buster
```